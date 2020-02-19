
import java.io.{ByteArrayOutputStream, File}
import java.util.Properties

import Utils.EpdData
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import libsvm.{svm, svm_node}
import ml.dmlc.xgboost4j.scala.XGBoost
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork


object Analytics {

  case class PreProcessed(key: String, uuid: String, recordTime: String, features: Array[Float])

  case class Result(key: String, uuid: String, recordTime: String, prediction: Int)

  case class ResultMessage(key: String, value: String)

  // Jackson Deserialize to type
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def jsonToType[T](json: String)(implicit m: Manifest[T]): T = {
    objectMapper.readValue[T](json)
  }

  // Flink KafkaConsumer Properties
  val consumerProps = new Properties()
  consumerProps.setProperty("bootstrap.servers", "192.168.21.10:9092")
  consumerProps.setProperty("group.id", "FlinkML")
  consumerProps.setProperty("max.poll.interval.ms", "500")
  consumerProps.setProperty("max.poll.records", "5")

  val producerProps = new Properties()
  producerProps.setProperty("bootstrap.servers", "192.168.21.10:9092")

  val consumerTopic = "flink01"
  val producerTopic = "epd01"

  // upload logistic regression DL4J model
  val lrFile = new File("lr-dl4j-model.zip")
  val lrModel = MultiLayerNetwork.load(lrFile, false)
  // upload gradient boosted trees and random forest XGBoost models
  val gbtModel = XGBoost.loadModel("xgb.model")
  val rfModel = XGBoost.loadModel("rf.model")
  // upload SVC model
  val svmModel = svm.svm_load_model("svm.epd.model")


  def runFlinkML {

    // start stream with KafkaConsumer as dataSourceStream 
    // create PreProcessed Records = One Hot Categories and normalized Sensors
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .addSource(new FlinkKafkaConsumer(consumerTopic, new DeserializationSchema, consumerProps))
      .map(record => {
        val epdData = jsonToType[EpdData](record.value)
        PreProcessed(record.key, epdData.uuid, epdData.CurrentTime, Utils.createDataRecord(epdData))
      })

    // process logistic regression filtered stream
    val lrStream = stream
      .filter((dataRecord: PreProcessed) => dataRecord.key == "LR")
      .map((dataRecord: PreProcessed) => {
        val ind = Utils.createINDArray(dataRecord.features)
        val probs = lrModel.output(ind)
        var prediction: Int = 0
        if (probs.getDouble(1L) > probs.getDouble(0L)) {
          prediction = 1
        }
        // println(s"Key: ${dataRecord.key} -- uuid: ${dataRecord.uuid} -- recordTime: ${dataRecord.recordTime} -- Prediction: $prediction")
        val out = new ByteArrayOutputStream()
        objectMapper.writeValue(out, Result(dataRecord.key, dataRecord.uuid, dataRecord.recordTime, prediction))
        ResultMessage(dataRecord.key, out.toString)
      })

    val gbtStream = stream
      .filter((dataRecord: PreProcessed) => dataRecord.key == "GBT")
      .map((dataRecord: PreProcessed) => {
        val dmatrix = Utils.createDMatrix(dataRecord.features)
        val output = gbtModel.predict(dmatrix)
        val prediction = Math.round(output(0)(0))
        val out = new ByteArrayOutputStream()
        objectMapper.writeValue(out, Result(dataRecord.key, dataRecord.uuid, dataRecord.recordTime, prediction))
        ResultMessage(dataRecord.key, out.toString)
      })

    val rfStream = stream
      .filter((dataRecord: PreProcessed) => dataRecord.key == "RF")
      .map((dataRecord: PreProcessed) => {
        val dmatrix = Utils.createDMatrix(dataRecord.features)
        val output = rfModel.predict(dmatrix)
        val prediction = Math.round(output(0)(0))
        val out = new ByteArrayOutputStream()
        objectMapper.writeValue(out, Result(dataRecord.key, dataRecord.uuid, dataRecord.recordTime, prediction))
        ResultMessage(dataRecord.key, out.toString)
      })

    val svcStream = stream
      .filter((dataRecord: PreProcessed) => dataRecord.key == "SVC")
      .map((dataRecord: PreProcessed) => {
        val nodes: Array[svm_node] = Array.fill(Utils.numFeature)(new svm_node())
        for (col <- 0 until Utils.numFeature) {
          nodes(col).index = col
          nodes(col).value = dataRecord.features(col)
        }
        val prediction = Math.round(svm.svm_predict(svmModel, nodes)).toInt
        val out = new ByteArrayOutputStream()
        objectMapper.writeValue(out, Result(dataRecord.key, dataRecord.uuid, dataRecord.recordTime, prediction))
        ResultMessage(dataRecord.key, out.toString)
      })

    val outputStream = lrStream.union(gbtStream, rfStream, svcStream)
    outputStream
      .map((msg: ResultMessage) => {
        println(s"Key: ${msg.key} -- Value: ${msg.value}")
        msg
      })
      .addSink(new FlinkKafkaProducer[ResultMessage](producerTopic, new SerializationSchema(producerTopic), producerProps, Semantic.EXACTLY_ONCE))

    env.execute("FlinkML")

  }


}
