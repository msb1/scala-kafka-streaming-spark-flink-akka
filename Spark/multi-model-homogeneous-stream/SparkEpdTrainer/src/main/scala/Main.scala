import com.mongodb.spark._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, ClusteringEvaluator}
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoderEstimator, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}


object Main {

  def main(args: Array[String]) {
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("SparkEpdTrainer")
      .config("spark.mongodb.input.uri", "mongodb://barnwaldo:shakeydog@192.168.21.10:27017/barnwaldo.epd01?authSource=admin")
      .config("spark.mongodb.output.uri", "mongodb://barnwaldo:shakeydog@192.168.21.10:27017/barnwaldo.epd01?authSource=admin")
      .getOrCreate()
    import spark.implicits._

    // schema to convert epd categories to ints (MongoSpark is reading as string rather than int)
//    val catSchema = new StructType()
//      .add("cat0", IntegerType)
//      .add("cat1", IntegerType)
//      .add("cat2", IntegerType)
//      .add("cat3", IntegerType)
//      .add("cat4", IntegerType)
//      .add("cat5", IntegerType)
//      .add("cat6", IntegerType)
//      .add("cat7", IntegerType)

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
    df.show(10, false)

    // columns to apply one hot encoding
    val categoryCols = Array("cat0", "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7")
    val oheCols = Array("cat0Vec", "cat1Vec", "cat2Vec", "cat3Vec", "cat4Vec", "cat5Vec", "cat6Vec", "cat7Vec")
    // columns after one hot encoding to be dropped after transforming dataframe to "features/label"
    val cols =  Array("cat0Vec", "cat1Vec", "cat2Vec", "cat3Vec", "cat4Vec", "cat5Vec", "cat6Vec", "cat7Vec",
                      "sensor0", "sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7", "sensor8", "sensor9")

    // Pre-process data
    val oneHotEncoder = new OneHotEncoderEstimator()
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
    pipelineModel.write.overwrite().save("/home/bw/scala/Spark/SparkEpdTrainer/pipeline-model")
    val reloadPipelineModel = PipelineModel.load("/home/bw/scala/Spark/SparkEpdTrainer/pipeline-model")

    val input = reloadPipelineModel.transform(df)
      .drop(cols: _*)
      .drop(categoryCols:_*)
      .drop("unscaled")
      .withColumnRenamed("Result","label")

    input.printSchema()
    input.show(10, false)

    // split data set training and test
    val seed = 42
    val Array(train, test) = input.randomSplit(Array(0.8, 0.2), seed)

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
    lrPrediction.show(10)

    // measure the accuracy
    val lrAccuracy = evaluator.evaluate(lrPrediction)
    println(f"Logistic Regression ROC acc: $lrAccuracy%1.3f")

    // save model
    lrModel.write.overwrite()
      .save("/home/bw/scala/Spark/SparkEpdTrainer/lr-test-model")

    // reload model
    val lrReloadModel = LogisticRegressionModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/lr-test-model")

    // test reloaded model with test data set
    val lrReloadPrediction = lrReloadModel.transform(test)
    lrReloadPrediction.show(10)
    val lrReloadAccuracy = evaluator.evaluate(lrReloadPrediction)
    println(f"Reloaded Logistic Regression ROC acc: $lrReloadAccuracy%1.3f")

    // *****************************************************************************************************************
    // train linear SUPPORT VECTOR CLASSIFIER model with training data set
    val svc = new LinearSVC()
      .setMaxIter(100)
      .setRegParam(0.1)

    val svcModel = svc.fit(train)

    // run svc model with test data set to get predictions
    val svcPrediction = svcModel.transform(test)
    println("--> Support Vector Classifier <--")
    svcPrediction.show(10)

    // measure the accuracy
    val svcAccuracy = evaluator.evaluate(svcPrediction)
    println(f"Support Vector Classifier ROC acc: $svcAccuracy%1.3f")

    // save model
    svcModel.write.overwrite()
      .save("/home/bw/scala/Spark/SparkEpdTrainer/svc-test-model")

    // reload model
    val svcReloadModel = LinearSVCModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/svc-test-model")

    // test reloaded model with test data set
    val svcReloadPrediction = svcReloadModel.transform(test)
    svcReloadPrediction.show(10)
    val svcReloadAccuracy = evaluator.evaluate(svcReloadPrediction)
    println(f"Reloaded Support Vector Classifier ROC acc: $svcReloadAccuracy%1.3f")

    // *****************************************************************************************************************
    // train linear RANDOM FOREST CLASSIFIER model with training data set
    val rf = new RandomForestClassifier()

    val rfModel = rf.fit(train)

    // run rf model with test data set to get predictions
    val rfPrediction = rfModel.transform(test)
    println("--> Random Forest Classifier <--")
    rfPrediction.show(10)

    // measure the accuracy
    val rfAccuracy = evaluator.evaluate(rfPrediction)
    println(f"Random Forest Classifier ROC acc: $rfAccuracy%1.3f")

    // save model
    rfModel.write.overwrite()
      .save("/home/bw/scala/Spark/SparkEpdTrainer/rf-test-model")

    // reload model
    val rfReloadModel = RandomForestClassificationModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/rf-test-model")

    // test reloaded model with test data set
    val rfReloadPrediction = rfReloadModel.transform(test)
    rfReloadPrediction.show(10)
    val rfReloadAccuracy = evaluator.evaluate(rfReloadPrediction)
    println(f"Reloaded Random Forest Classifier ROC acc: $rfReloadAccuracy%1.3f")

    // *****************************************************************************************************************
    // train linear GRADIENT BOOSTED TREE CLASSIFIER model with training data set
    val gbt = new GBTClassifier()
      .setMaxIter(20)
      .setFeatureSubsetStrategy("auto")

    val gbtModel = gbt.fit(train)

    // run logistic regression model with test data set to get predictions
    val gbtPrediction = gbtModel.transform(test)
    println("--> Gradient Boosted Tree Classifier <--")
    gbtPrediction.show(10)

    // measure the accuracy
    val gbtAccuracy = evaluator.evaluate(gbtPrediction)
    println(f"Gradient Boosted Tree Classifier ROC acc: $gbtAccuracy%1.3f")

    // save model
    gbtModel.write.overwrite()
      .save("/home/bw/scala/Spark/SparkEpdTrainer/gbt-test-model")

    // reload model
    val gbtReloadModel = GBTClassificationModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/gbt-test-model")

    // test reloaded model with test data set
    val gbtReloadPrediction = gbtReloadModel.transform(test)
    gbtReloadPrediction.show(10)
    val gbtReloadAccuracy = evaluator.evaluate(gbtReloadPrediction)
    println(f"Reloaded Gradient Boosted Tree Classifier ROC acc: $gbtReloadAccuracy%1.3f")

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

    // transform dataframe to prepare for Logistic Regression Test/Train
    //val cluster = assembler
    //  .setHandleInvalid("skip")
    //  .transform(df.select($"Categories.*", $"Sensors.*"))
    //  .drop(cols: _*)

    val kMeans = new KMeans().setK(2).setSeed(1L)
    // val kMeansModel = kMeans.fit(cluster)
    val kMeansModel = kMeans.fit(input.select($"features"))

    // Make predictions
    // val kMeansPredictions = kMeansModel.transform(cluster)
    val kMeansPredictions = kMeansModel.transform(input.select($"features"))

    // Evaluate clustering by computing Silhouette score
    val clusterEvaluator = new ClusteringEvaluator()

    println("--> KMeans Clustering <--")
    df.groupBy("Result").count.show()
    kMeansPredictions.groupBy("prediction").count().show()
    val silhouette = clusterEvaluator.evaluate(kMeansPredictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
    // Shows the result.
    println("Cluster Centers: \n")
    kMeansModel.clusterCenters.foreach(ctr => println(ctr + "\n"))

    // save model
    kMeansModel.write.overwrite()
      .save("/home/bw/scala/Spark/SparkEpdTrainer/kmeans-test-model")

    // reload model
    val kMeansReloadModel = KMeansModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/kmeans-test-model")

    // Make predictions
    val kMeansReloadPredictions = kMeansReloadModel.transform(input.select($"features"))

    println("--> KMeans Clustering Reloaded <--")
    df.groupBy("Result").count.show()
    kMeansReloadPredictions.groupBy("prediction").count().show()
    val reloadSilhouette = clusterEvaluator.evaluate(kMeansReloadPredictions)
    println(s"Reload Silhouette with squared euclidean distance = $reloadSilhouette")
    // Shows the result.
    println("Reload Cluster Centers: \n")
    kMeansReloadModel.clusterCenters.foreach(ctr => println(ctr + "\n"))
  }

}
