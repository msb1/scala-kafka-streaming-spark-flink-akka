import java.io.{BufferedWriter, File, FileWriter}

import Utils.EquipConfig
import com.mongodb.spark._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, ClusteringEvaluator}
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession


object Main {

  def main(args: Array[String]) {

    val equipFileName = args(0)
    val equipFilePath = "/home/bw/data/equipconfig/"
    val outputFilePath = "/home/bw/data/sparkml/"
    val equipConfig: EquipConfig = Utils.readConfigData(equipFilePath + equipFileName + ".yml")
    println(equipConfig)
    val evalFile = new File(outputFilePath + equipConfig.name + ".eval.txt")
    val bw = new BufferedWriter(new FileWriter(evalFile))

    val dbCol: String = "equip." + equipConfig.name
    val mongoPath: String = "mongodb://barnwaldo:shakeydog@xxx.xxx.xxx.x:27017/" + dbCol + "?authSource=admin"
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("SparkMLTrainer")
      .config("spark.mongodb.input.uri", mongoPath)
      .config("spark.mongodb.output.uri", mongoPath)
      .getOrCreate()
    import spark.implicits._

    val dfMongo = MongoSpark.load(spark)

    val df = dfMongo
      .select(
        dfMongo.col("CurrentTime"),
        dfMongo.col("Result"),
        dfMongo.col("Categories"),
        dfMongo.col("Sensors")
      )
      .select($"CurrentTime", $"Result", $"Categories.*", $"Sensors.*")

    df.printSchema()
    df.show(10, truncate = false)

    // columns to apply one hot encoding
    val sensorCols: Array[String] = equipConfig.cont.map(s => s.name)
    val categoryCols: Array[String] = equipConfig.disc.map(d => d.name)
    val oheCols: Array[String] = categoryCols.map(c => c + "Vec")

    // columns after one hot encoding to be dropped after transforming dataframe to "features/label"
    val cols: Array[String] =  oheCols ++ sensorCols

    // Pre-process data
    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(categoryCols)
      .setOutputCols(oheCols)

    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("unscaled")

    val scaler = new MinMaxScaler()
      .setInputCol("unscaled")
      .setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(oneHotEncoder, assembler, scaler))
    val pipelineModel = pipeline.fit(df)
    pipelineModel.write.overwrite().save(outputFilePath + "pipeline-spark-" + equipConfig.name)
    val reloadPipelineModel = PipelineModel.load(outputFilePath + "pipeline-spark-" + equipConfig.name)



    val input = reloadPipelineModel.transform(df)
      .drop(cols: _*)
      .drop(categoryCols:_*)
      .drop("unscaled")
      .withColumnRenamed("Result","label")

    input.printSchema()
    input.show(10, truncate = false)

    // split data set training and test
    val seed = 42
    val Array(train, test) = input.randomSplit(Array(0.9, 0.1), seed)

    // define binary classification evaluator
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    // *****************************************************************************************************************
    // train LOGISTIC REGRESSION model with training data set
    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(train)

    // run logistic regression model with test data set to get predictions
    val lrPrediction = lrModel.transform(test)
    println("--> Logistic Regression <--")
    bw.write("--> Logistic Regression <-- \n")
    lrPrediction.show(10)

    // measure the accuracy
    val lrAccuracy = evaluator.evaluate(lrPrediction)
    println(f"Logistic Regression ROC acc: $lrAccuracy%1.3f")
    bw.write(f"Logistic Regression ROC acc: $lrAccuracy%1.3f \n")

    // save model
    lrModel.write.overwrite()
      .save(outputFilePath + "lr-spark-" + equipConfig.name)

    // reload model
    val lrReloadModel = LogisticRegressionModel
      .load(outputFilePath + "lr-spark-" + equipConfig.name)

    // test reloaded model with test data set
    val lrReloadPrediction = lrReloadModel.transform(test)
    lrReloadPrediction.show(10)
    val lrReloadAccuracy = evaluator.evaluate(lrReloadPrediction)
    println(f"Reloaded Logistic Regression ROC acc: $lrReloadAccuracy%1.3f")
    println(f"Reloaded Logistic Regression ROC acc: $lrReloadAccuracy%1.3f \n")
    bw.write(f"Reloaded Logistic Regression ROC acc: $lrReloadAccuracy%1.3f \n")
    bw.write(f"Reloaded Logistic Regression ROC acc: $lrReloadAccuracy%1.3f \n\n")

    // *****************************************************************************************************************
    // train linear SUPPORT VECTOR CLASSIFIER model with training data set
    val svc = new LinearSVC()
      .setMaxIter(100)
      .setRegParam(0.1)

    val svcModel = svc.fit(train)

    // run svc model with test data set to get predictions
    val svcPrediction = svcModel.transform(test)
    println("--> Support Vector Classifier <--")
    bw.write("--> Support Vector Classifier <-- \n")
    svcPrediction.show(10)
    bw.write(svcPrediction.show(10).toString)

    // measure the accuracy
    val svcAccuracy = evaluator.evaluate(svcPrediction)
    println(f"Support Vector Classifier ROC acc: $svcAccuracy%1.3f")
    bw.write(f"Support Vector Classifier ROC acc: $svcAccuracy%1.3f \n")

    // save model
    svcModel.write.overwrite()
      .save(outputFilePath + "svc-spark-" + equipConfig.name)

    // reload model
    val svcReloadModel = LinearSVCModel
      .load(outputFilePath + "svc-spark-" + equipConfig.name)

    // test reloaded model with test data set
    val svcReloadPrediction = svcReloadModel.transform(test)
    svcReloadPrediction.show(10)
    val svcReloadAccuracy = evaluator.evaluate(svcReloadPrediction)
    println(f"Reloaded Support Vector Classifier ROC acc: $svcReloadAccuracy%1.3f")
    bw.write(f"Reloaded Support Vector Classifier ROC acc: $svcReloadAccuracy%1.3f \n")

    // *****************************************************************************************************************
    // train linear RANDOM FOREST CLASSIFIER model with training data set
    val rf = new RandomForestClassifier()

    val rfModel = rf.fit(train)

    // run rf model with test data set to get predictions
    val rfPrediction = rfModel.transform(test)
    println("--> Random Forest Classifier <--")
    bw.write("--> Random Forest Classifier <-- \n")
    rfPrediction.show(10)

    // measure the accuracy
    val rfAccuracy = evaluator.evaluate(rfPrediction)
    println(f"Random Forest Classifier ROC acc: $rfAccuracy%1.3f")
    bw.write(f"Random Forest Classifier ROC acc: $rfAccuracy%1.3f \n")

    // save model
    rfModel.write.overwrite()
      .save(outputFilePath + "rf-spark-" + equipConfig.name)

    // reload model
    val rfReloadModel = RandomForestClassificationModel
      .load(outputFilePath + "rf-spark-" + equipConfig.name)

    // test reloaded model with test data set
    val rfReloadPrediction = rfReloadModel.transform(test)
    rfReloadPrediction.show(10)
    val rfReloadAccuracy = evaluator.evaluate(rfReloadPrediction)
    println(f"Reloaded Random Forest Classifier ROC acc: $rfReloadAccuracy%1.3f")
    bw.write(f"Reloaded Random Forest Classifier ROC acc: $rfReloadAccuracy%1.3f \n")

    // *****************************************************************************************************************
    // train linear GRADIENT BOOSTED TREE CLASSIFIER model with training data set
    val gbt = new GBTClassifier()
      .setMaxIter(20)
      .setFeatureSubsetStrategy("auto")

    val gbtModel = gbt.fit(train)

    // run logistic regression model with test data set to get predictions
    val gbtPrediction = gbtModel.transform(test)
    println("--> Gradient Boosted Tree Classifier <--")
    bw.write("--> Gradient Boosted Tree Classifier <-- \n")
    gbtPrediction.show(10)

    // measure the accuracy
    val gbtAccuracy = evaluator.evaluate(gbtPrediction)
    println(f"Gradient Boosted Tree Classifier ROC acc: $gbtAccuracy%1.3f")
    bw.write(f"Gradient Boosted Tree Classifier ROC acc: $gbtAccuracy%1.3f \n")

    // save model
    gbtModel.write.overwrite()
      .save(outputFilePath + "gbt-spark-" + equipConfig.name)

    // reload model
    val gbtReloadModel = GBTClassificationModel
      .load(outputFilePath + "gbt-spark-" + equipConfig.name)

    // test reloaded model with test data set
    val gbtReloadPrediction = gbtReloadModel.transform(test)
    gbtReloadPrediction.show(10)
    val gbtReloadAccuracy = evaluator.evaluate(gbtReloadPrediction)
    println(f"Reloaded Gradient Boosted Tree Classifier ROC acc: $gbtReloadAccuracy%1.3f")
    bw.write(f"Reloaded Gradient Boosted Tree Classifier ROC acc: $gbtReloadAccuracy%1.3f \n")

    // *****************************************************************************************************************
    // Statistics of Train Data and K-Means clustering (without results column)
    val stats = dfMongo.select(
      dfMongo.col("Categories"),
      dfMongo.col("Sensors"),
      dfMongo.col("Result"))

    println("--> Train Data Statistics <--")
    stats.select($"Result").describe().show()
    stats.select($"Categories.*").describe().show()
    stats.select($"Sensors.*").describe().show()

    val kMeans = new KMeans().setK(2).setSeed(1L)
    val kMeansModel = kMeans.fit(input.select($"features"))

    // Make predictions
    val kMeansPredictions = kMeansModel.transform(input.select($"features"))

    // Evaluate clustering by computing Silhouette score
    val clusterEvaluator = new ClusteringEvaluator()

    println("--> KMeans Clustering <--")
    bw.write("--> KMeans Clustering <-- \n")

    df.groupBy("Result").count.show()
    kMeansPredictions.groupBy("prediction").count().show()
    val silhouette = clusterEvaluator.evaluate(kMeansPredictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
    bw.write(s"Silhouette with squared euclidean distance = $silhouette \n")
    // Shows the result.
    println("Cluster Centers: \n")
    bw.write("Cluster Centers: \n\n")
    kMeansModel.clusterCenters.foreach(ctr => {
      println(ctr + "\n")
      bw.write(ctr + "\n")
    })

    // save model
    kMeansModel.write.overwrite()
      .save(outputFilePath + "kmeans-spark-" + equipConfig.name)

    // reload model
    val kMeansReloadModel = KMeansModel
      .load(outputFilePath + "kmeans-spark-" + equipConfig.name)

    // Make predictions
    val kMeansReloadPredictions = kMeansReloadModel.transform(input.select($"features"))

    println("--> KMeans Clustering Reloaded <--")
    bw.write("--> KMeans Clustering Reloaded <-- \n")
    df.groupBy("Result").count.show()
    kMeansReloadPredictions.groupBy("prediction").count().show()
    val reloadSilhouette = clusterEvaluator.evaluate(kMeansReloadPredictions)
    println(s"Reload Silhouette with squared euclidean distance = $reloadSilhouette")
    bw.write(s"Reload Silhouette with squared euclidean distance = $reloadSilhouette \n")
    // Shows the result.
    println("Reload Cluster Centers: \n")
    bw.write("Reload Cluster Centers: \n\n")
    kMeansReloadModel.clusterCenters.foreach(ctr => {
      println(ctr + "\n")
      bw.write(ctr + "\n")
    })
    bw.close()
    spark.stop()
  }
}
