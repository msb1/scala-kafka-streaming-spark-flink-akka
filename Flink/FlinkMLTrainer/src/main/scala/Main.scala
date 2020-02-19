import java.io.File

import libsvm.{svm, svm_node, svm_parameter, svm_problem}
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.datavec.api.records.reader.impl.csv.CSVRecordReader
import org.datavec.api.split.FileSplit
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.OutputLayer
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.{EvaluativeListener, ScoreIterationListener}
import org.nd4j.evaluation.classification.Evaluation
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.learning.config.Nesterovs

import scala.collection.immutable.HashMap
import scala.io.Source

object Main extends App {
  println("Start Flink ML Trainer with DL4j, XGBoost and LibSVM...")
  println("\n***********************************************************************************************************\n")
  /**
   * *******************************************************************************************************************
   * Read training data from MongoDB and create svmlight and csv files
   * *******************************************************************************************************************
   */
  val epdData = Utils.createEpdDataFiles()

  /**
   * *******************************************************************************************************************
   * Logistic Regression with DL4J fully connected layer
   * *******************************************************************************************************************
   */
  println("\n***********************************************************************************************************\n")
  println("Evaluate Deeplearning4j Logistic Regression (One fully connected layer)")
  val labelIndex = 30
  val numFeatures = 30
  val numClasses = 2 // binary classification
  val batchSize = 10

  val seed = 42
  val numEpochs = 20
  val testTrainSplit = 0.8
  val splitIndex: Int = (testTrainSplit * epdData.length).toInt

  // training data
  val numLinesToSkip = 0
  val delimiter = ','
  val trainRR = new CSVRecordReader(numLinesToSkip, delimiter)
  trainRR.initialize(new FileSplit(new File("epdDataTrain.csv")))
  val train = new RecordReaderDataSetIterator(trainRR, batchSize, labelIndex, numClasses)

  val testRR = new CSVRecordReader(numLinesToSkip, delimiter)
  testRR.initialize(new FileSplit(new File("epdDataTest.csv")))
  val test = new RecordReaderDataSetIterator(testRR, batchSize, labelIndex, numClasses)

  println(s"TRAIN columns:  ${train.inputColumns().toString}     TEST columns:  ${test.inputColumns().toString}")
  println(s"TRAIN outcomes: ${train.totalOutcomes().toString}      TEST outcomes: ${test.totalOutcomes().toString}")

  // output layer
  val outputLayer: OutputLayer = new OutputLayer.Builder()
    .nIn(numFeatures)
    .nOut(numClasses)
    .weightInit(WeightInit.XAVIER)
    .activation(Activation.SOFTMAX)
    .build()

  // logistic regression (equivalent to FCN NN)
  val conf: MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
    .seed(seed).optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).updater(new Nesterovs(0.9))
    .list()
    .layer(0, outputLayer)
    .build() //Building Configuration

  val lrmodel = new MultiLayerNetwork(conf)
  lrmodel.init()

  println("Train logistic regression DL4J model....")
  lrmodel.setListeners(new ScoreIterationListener(10), new EvaluativeListener(test, 10))
  lrmodel.fit(train, numEpochs)

  println("Check predictions for logistic regression DL4J model....")
  val eval: Evaluation = lrmodel.evaluate(test)
  println(eval.stats())

  println("Save logistic regression DL4j model...")
  val lrFile = new File("lr-dl4j-model.zip")
  lrmodel.save(lrFile, false)

  val lrmodelReload = MultiLayerNetwork.load(lrFile, false)

  println("Check predictions for reloaded logistic regression DL4j model...")
  //  for (i <- 0 until epdData.length if i % 100 == 0) {
  //    val record = epdData(i)
  //    val indRecord = Utils.createINDArray(record, numFeatures)
  //    val probs = lrModelReload.output(indRecord)
  //    var prediction: Int = 0
  //    if (probs.getDouble(1L) > probs.getDouble(0L)) {
  //      prediction = 1
  //    }
  //    println(f"Prediction: $prediction%d --- Actual: ${record.Result}%d -- Probabilities: ${probs.getDouble(0L)}%.3f, ${probs.getDouble(1L)}%.3f")
  //  }
  val evalReload: Evaluation = lrmodelReload.evaluate(test)
  println(evalReload.stats())

  /**
   * *******************************************************************************************************************
   * Gradient Boosted Trees with XGBoost
   * *******************************************************************************************************************
   */
  println("\n***********************************************************************************************************\n")
  println("Evaluate Gradient Boosted Tree XGBoost model....")
  // Read in test/train data in svmlight format to DMatrices
  val trainDMatrix = new DMatrix("epdData.train.svm.txt")
  val testDMatrix = new DMatrix("epdData.test.svm.txt")

  // train boosted tree model
  val rounds = 5
  val params: HashMap[String, Any] = HashMap("eta" -> 0.3, "max_depth" -> 3, "silent" -> 1, "objective" -> "binary:logistic", "eval_metric" -> "auc")
  val watches: HashMap[String, DMatrix] = HashMap("train" -> trainDMatrix, "test" -> testDMatrix)
  val metrics: Array[Array[Float]] = Array(Array(0, 0, 0, 0, 0), Array(0, 0, 0, 0, 0))
  val xgbModel = XGBoost.train(trainDMatrix, params, rounds, watches, metrics)

  // use xgbModel to predict results
  val xgbPredict = xgbModel.predict(testDMatrix)

  // save model to file
  println("Save Gradient Boosted Tree XGBoost model....")
  xgbModel.saveModel("xgb.model")

  println("Predictions for Gradient Boosted Tree XGBoost model....")
  println("Feature Score:")
  println(xgbModel.getFeatureScore("featmap.txt") + "\n")
  println("Gain:")
  println(xgbModel.getScore("featmap.txt", "gain") + "\n")
  println("Cover:")
  println(xgbModel.getScore("featmap.txt", "cover") + "\n")

  for (i <- 0 until 5) {
    println(f"Training Round $i%d: Train AUC = ${metrics(0)(i)}%.3f   Test AUC = ${metrics(1)(i)}%.3f")
  }

  // reload model and data
  val xgbModelReloaded = XGBoost.loadModel("xgb.model")
  val xgbPredictReloaded = xgbModelReloaded.predict(testDMatrix)

  // check predictions from reloaded model
  var xgbCtr: Double = 0.0
  var xgbRCtr: Double = 0.0
  for (i <- 0 until xgbPredict.length) {
    val actual = epdData(i * 5).Result
    if (Math.round(xgbPredict(i)(0)) == actual) {
      xgbCtr += 1.0
    }
    if (Math.round(xgbPredictReloaded(i)(0)) == actual) {
      xgbRCtr += 1.0
    }
  }
  val xgbAcc = xgbCtr / xgbPredict.length
  val xgbAccR = xgbRCtr / xgbPredict.length

  println(f"\nPredicted Accuracies: $xgbAcc%.3f, $xgbAccR%.3f  (Predicted, Reloaded Predicted)")

  /**
   * *******************************************************************************************************************
   * Random Forest with XGBoost
   * *******************************************************************************************************************
   */
  println("\n***********************************************************************************************************\n")
  println("Evaluate Random Forest XGBoost model....")

  // train boosted tree model
  val rfParams: HashMap[String, Any] = HashMap("eta" -> 1.0, "max_depth" -> 3, "silent" -> 1, "objective" -> "binary:logistic", "eval_metric" -> "auc",
    "subsample" -> 0.8, "tree_method" -> "hist", "num_parallel_tree" -> 100, "colsample_bynode" -> "0.8",
    "learning_rate" -> 1, "colsample_bynode" -> 0.8, "num_boost_round" -> 1)
  val rfModel = XGBoost.train(trainDMatrix, rfParams, rounds, watches, metrics)

  // use xgbModel to predict results
  val rfPredict = rfModel.predict(testDMatrix)

  // save model to file
  println("Save Random Forest XGBoost model....")
  xgbModel.saveModel("rf.model")

  println("Predictions for Random Forest XGBoost model....")
  println("Feature Score:")
  println(rfModel.getFeatureScore("featmap.txt") + "\n")
  println("Gain:")
  println(rfModel.getScore("featmap.txt", "gain") + "\n")
  println("Cover:")
  println(rfModel.getScore("featmap.txt", "cover") + "\n")

  for (i <- 0 until 5) {
    println(f"Training Round $i%d: Train AUC = ${metrics(0)(i)}%.3f   Test AUC = ${metrics(1)(i)}%.3f")
  }

  // reload model and data
  val rfModelReloaded = XGBoost.loadModel("rf.model")
  val rfPredictReloaded = xgbModelReloaded.predict(testDMatrix)

  // check predictions from reloaded model
  var rfCtr: Double = 0.0
  var rfRCtr: Double = 0.0
  for (i <- 0 until xgbPredict.length) {
    val actual = epdData(i * 5).Result
    if (Math.round(rfPredict(i)(0)) == actual) {
      rfCtr += 1.0
    }
    if (Math.round(rfPredictReloaded(i)(0)) == actual) {
      rfRCtr += 1.0
    }
  }
  val rfAcc = rfCtr / rfPredict.length
  val rfAccR = rfRCtr / rfPredict.length

  println(f"\nPredicted Accuracies: $rfAcc%.3f, $rfAccR%.3f  (Predicted, Reloaded Predicted)")

  /**
   * *******************************************************************************************************************
   * Support Vector Machine from LIBSVM Java API
   * *******************************************************************************************************************
   */
  println("\n***********************************************************************************************************\n")
  println("Evaluate LIBSVM model....")
  // Define svm model parameters (use defaults mostly)
  val param = new svm_parameter()
  param.svm_type = svm_parameter.C_SVC
  param.kernel_type = svm_parameter.RBF
  param.gamma = 1.0 / numFeatures
  param.C = 2.0
  param.eps = 1e-4
  param.cache_size = 500
  param.probability = 0
  val cross_validation = 0

  // define problem
  val problem = new svm_problem()
  problem.l = (0.8 * epdData.length).toInt
  problem.y = Array.ofDim[Double](problem.l)
  problem.x = Array.ofDim[svm_node](problem.l, numFeatures)
  val trainFilename = "epdData.train.svm.txt"
  var trainNum: Int = 0
  for (line <- Source.fromFile(trainFilename).getLines) {
    val tokens = line.split(' ')
    problem.y(trainNum) = tokens(0).toInt
    for (col <- 0 until numFeatures) {
      val node = new svm_node()
      node.index = col
      node.value = tokens(col + 1).split(':')(1).toFloat
      problem.x(trainNum)(col) = node
    }
    trainNum += 1
  }
  println(s"Number of training samples: $trainNum  (${problem.l})")

  val errorMessage = svm.svm_check_parameter(problem, param)
  if (errorMessage != null) {
    System.err.print("ERROR: " + errorMessage + "\n")
    System.exit(1)
  }

  println("Train LIBSVM model and save to file....")
  // train model
  val svmModelFilename = "svm.epd.model"
  val svmModel = svm.svm_train(problem, param);
  svm.svm_save_model(svmModelFilename, svmModel);

  // reload model to check integrity
  val svmModelReloaded = svm.svm_load_model(svmModelFilename)

  // Make predictions and check model results
  var trainCtr = 0
  trainNum = 0
  for (line <- Source.fromFile(trainFilename).getLines) {
    val tokens = line.split(' ')
    val result = tokens(0).toInt
    val nodes: Array[svm_node] = Array.fill(numFeatures)(new svm_node())
    for (col <- 0 until numFeatures) {
      nodes(col).index = col
      nodes(col).value = tokens(col + 1).split(':')(1).toFloat
    }
    val prediction = Math.round(svm.svm_predict(svmModel, nodes)).toInt
    if (prediction == result) {
      trainCtr += 1
    }
    trainNum += 1
  }

  var testCtr = 0
  var testReloadCtr = 0
  val testFilename = "epdData.test.svm.txt"
  var testNum = 0
  for (line <- Source.fromFile(testFilename).getLines) {
    val tokens = line.split(' ')
    val result = tokens(0).toInt
    val nodes: Array[svm_node] = Array.fill(numFeatures)(new svm_node())
    for (col <- 0 until numFeatures) {
      nodes(col).index = col
      nodes(col).value = tokens(col + 1).split(':')(1).toFloat
    }
    val prediction = Math.round(svm.svm_predict(svmModel, nodes)).toInt
    if (prediction == result) {
      testCtr += 1
    }
    val predictionReloaded = Math.round(svm.svm_predict(svmModelReloaded, nodes)).toInt
    if (predictionReloaded == result) {
      testReloadCtr += 1
    }
    testNum += 1
  }

  println(s"Number of test samples: $testNum    Counters: $trainCtr   $testCtr   $testReloadCtr")
  println("LIBSVM model Results:")
  val trainAcc: Double = trainCtr.toDouble / trainNum
  val testAcc: Double = testCtr.toDouble / testNum
  val testReloadAcc: Double = testReloadCtr.toDouble / testNum
  println(f"SVM accuracies:  Train = $trainAcc%.3f   Test = $testAcc%.3f  Test(reloaded) = $testReloadAcc%.3f")

  println("\n******************************************** FINISHED *****************************************************\n")
  System.exit(0)
}

