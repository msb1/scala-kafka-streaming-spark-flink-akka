package Broker

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{GBTClassificationModel, LinearSVCModel, LogisticRegressionModel, RandomForestClassificationModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

object SparkMLConsumer {

  val logger = LoggerFactory.getLogger("SparkMLConsumer")
  val batch = 10

  // case class for simulated data
  case class EpdData(CurrentTime: String, Topic: String, Categories: HashMap[String, Int], Sensors: HashMap[String, Double], Result: Int)

  // Spark SQL struct for simulated data
  val epdSchema = new StructType()
    .add("uuid", StringType)
    .add("CurrentTime", StringType)
    .add("Topic", StringType)
    .add("Result", IntegerType)
    .add("Categories",
      new StructType()
        .add("cat0", IntegerType)
        .add("cat1", IntegerType)
        .add("cat2", IntegerType)
        .add("cat3", IntegerType)
        .add("cat4", IntegerType)
        .add("cat5", IntegerType)
        .add("cat6", IntegerType)
        .add("cat7", IntegerType)
    )
    .add("Sensors",
      new StructType()
        .add("sensor0", DoubleType)
        .add("sensor1", DoubleType)
        .add("sensor2", DoubleType)
        .add("sensor3", DoubleType)
        .add("sensor4", DoubleType)
        .add("sensor5", DoubleType)
        .add("sensor6", DoubleType)
        .add("sensor7", DoubleType)
        .add("sensor8", DoubleType)
        .add("sensor9", DoubleType)
    )

  def runSparkConsumer(ctopic: String, ptopic: String, bootstrapServer: String): Unit = {
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("SparkMLConsumer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Subscribe to one topic with spark kafka connector
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", ctopic)
      .option("startingOffsets", "latest")
      .load()

    // reload trained models
    // (0) Pre-Process Pipeline
    val pipelineModel = PipelineModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/pipeline-model")

    // (1) Logistic Regression
    val lrModel = LogisticRegressionModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/lr-test-model")

    // (2) Linear SVM Classifier
    val svcModel = LinearSVCModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/svc-test-model")

    // (3) Random Forest
    val rfModel = RandomForestClassificationModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/rf-test-model")

    // (4) Gradient Boosted Trees
    val gbtModel = GBTClassificationModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/gbt-test-model")

    // columns to drop after preprocessing
    val categoryCols = Array("cat0", "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7")
    val cols = Array("cat0Vec", "cat1Vec", "cat2Vec", "cat3Vec", "cat4Vec", "cat5Vec", "cat6Vec", "cat7Vec",
      "sensor0", "sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7", "sensor8", "sensor9")

    val input = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select($"key", from_json($"value", epdSchema).as("data"))
      .select($"key", $"data.uuid", $"data.CurrentTime", $"data.Result", $"data.Categories.*", $"data.Sensors.*")

    val query = pipelineModel
      .transform(input)
      .drop(cols: _*)
      .drop(categoryCols: _*)
      .drop("unscaled")
      .withColumnRenamed("Result", "label")

    // process data with trained models and send to Kafka Producer
    val outputLR = lrModel.transform(query.filter($"key" === "LR"))
      .select($"key", $"uuid", $"CurrentTime", $"label", $"rawPrediction", $"prediction")
      .selectExpr("key", "to_json(struct(*)) AS value")

    val outputSVC = svcModel.transform(query.filter($"key" === "SVC"))
      .select($"key", $"uuid", $"CurrentTime", $"label", $"rawPrediction", $"prediction")
      .selectExpr("key", "to_json(struct(*)) AS value")

    val outputRF = rfModel.transform(query.filter($"key" === "RF"))
      .select($"key", $"uuid", $"CurrentTime", $"label", $"rawPrediction", $"prediction")
      .selectExpr("key", "to_json(struct(*)) AS value")

    val outputGBT = gbtModel.transform(query.filter($"key" === "GBT"))
      .select($"key", $"uuid", $"CurrentTime", $"label", $"rawPrediction", $"prediction")
      .selectExpr("key", "to_json(struct(*)) AS value")

    val cumulativeOutput = outputLR.union(outputSVC).union(outputRF).union(outputGBT)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", ptopic)
      .option("checkpointLocation", "checkpoints")
      .start()

    cumulativeOutput.awaitTermination(3600000)
    cumulativeOutput.stop()
  }

}
