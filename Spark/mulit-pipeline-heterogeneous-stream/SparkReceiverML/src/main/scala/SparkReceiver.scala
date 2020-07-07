import java.util.Properties

import Utils.FileUtils.EpdConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LinearSVCModel
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object SparkReceiver {

  val logger: Logger = LoggerFactory.getLogger("Utils.SparkMLConsumer")

  // Kafka Producer properties map
  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "xxx.xxx.xxx.x:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "1")


  def process(epdConfig: Array[EpdConfig], producerTopic: String, consumerTopic: String,
              bootstrapServer: String, sparkmlFilePath: String): Unit = {
    // build schema for each equipment model
    val epdSchema: Array[StructType] = epdConfig.map(epd => {
      val catField: Array[StructField] = epd.disc.map(d => StructField(d.name, IntegerType, nullable = true)).toArray
      val sensField: Array[StructField] = epd.cont.map(s => StructField(s.name, DoubleType, nullable = true)).toArray
      new StructType()
        .add("uuid", StringType)
        .add("currentTime", StringType)
        .add("topic", StringType)
        .add("result", IntegerType)
        .add("categories", new StructType(catField))
        .add("sensors", new StructType(sensField))
    })

    // build columns for schema manipulation
    var categoryCols: mutable.HashMap[String, List[String]] = mutable.HashMap.empty[String, List[String]]
    var cols: mutable.HashMap[String, List[String]] = mutable.HashMap.empty[String, List[String]]
    epdConfig.foreach(epd => {
      val sensorCols: List[String] = epd.cont.map(s => s.name)
      categoryCols += (epd.name -> epd.disc.map(d => d.name))
      val oheCols: List[String] = categoryCols(epd.name).map(c => c + "Vec")
      cols += (epd.name -> (oheCols ++ sensorCols))
    })

    // read in pipeline and trained SVC models from file for each piece of equipment
    val pipelineModel: Array[PipelineModel] = epdConfig.map(epd => PipelineModel.load(sparkmlFilePath + "pipeline-spark-" + epd.name))
    val svcModel: Array[LinearSVCModel] = epdConfig.map(epd => LinearSVCModel.load(sparkmlFilePath + "svc-spark-" + epd.name))

    // create or get SparkSessions
    val spark: SparkSession = SparkSession
      .builder
      .master("local[4]")
      .appName("SparkMLReceiver")
      // .config("checkpointLocation", "/data/spark-checkpoints")
      .config("forceDeleteTempCheckpointLocation", value = true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    import spark.implicits._

    // get dataframe (stream) from kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", consumerTopic)
      .option("startingOffsets", "latest")
      .load()

    // split dataframes into
    val input: Array[DataFrame] = epdConfig.zipWithIndex.map(x => {
      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
        .filter($"key" === x._1.name)
        .select($"key", from_json($"value", epdSchema(x._2)).as("data"))
        .select($"key", $"data.uuid", $"data.currentTime", $"data.result", $"data.categories.*", $"data.sensors.*")
    })

    val query: Array[DataFrame] = epdConfig.zipWithIndex.map(x => {
      pipelineModel(x._2)
        .transform(input(x._2))
        .drop(cols(x._1.name): _*)
        .drop(categoryCols(x._1.name): _*)
        .drop("unscaled")
        .withColumnRenamed("Result", "label")
    })

    val svc: Array[DataFrame] = epdConfig.zipWithIndex.map(x => {
      svcModel(x._2).transform(query(x._2))
        .select($"key", $"uuid", $"currentTime", $"label", $"rawPrediction", $"prediction")
        .selectExpr("key", "to_json(struct(*)) AS value")
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    })

    val output = svc.reduce(_ union _)
    val stream: StreamingQuery = output
      .writeStream.foreach(
      new ForeachWriter[Row] {


        def open(partitionId: Long, version: Long): Boolean = {
          true
        }

        def process(row: Row): Unit = {
          val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
          val record = new ProducerRecord[String, String](producerTopic, row.getString(0), row.getString(1))
          producer.send(record)
        }

        def close(errorOrNull: Throwable): Unit = {
        }
      }
    ).start()
    stream.awaitTermination()
    stream.stop()
  }
}
