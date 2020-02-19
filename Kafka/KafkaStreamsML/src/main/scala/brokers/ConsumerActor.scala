package actors

import java.io.{ByteArrayOutputStream, File}

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape}
import broker.KafkaProducerClass.PMessage
import brokers.StreamHelper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import libsvm.{svm, svm_node}
import ml.dmlc.xgboost4j.scala.XGBoost
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

class KafkaConsumerActor(producer: ActorRef) extends Actor {

  // define ActorSystem and Materializer for akka streams
  implicit val system = ActorSystem("KafkaStreamsML")
  implicit val materializer = ActorMaterializer()
  val logger = LoggerFactory.getLogger("KafkaConsumerActor")

  import KafkaConsumerClass._

  // config for akka Kafka Consumer
  val conConfig = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, String] = ConsumerSettings(conConfig, new StringDeserializer, new StringDeserializer)
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

  // Jackson Json to/from type
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def jsonToType[T](json: String)(implicit m: Manifest[T]): T = {
    objectMapper.readValue[T](json)
  }

  // upload logistic regression DL4J model
  val lrFile = new File("lr-dl4j-model.zip")
  val lrModel = MultiLayerNetwork.load(lrFile, false)
  // upload gradient boosted trees and random forest XGBoost models
  val gbtModel = XGBoost.loadModel("xgb.model")
  val rfModel = XGBoost.loadModel("rf.model")
  // upload SVC model
  val svmModel = svm.svm_load_model("svm.epd.model")

  var consumerRunning = false

  def receive: Receive = {
    case RunConsumer(consumerTopic: String, producerTopic: String) => {
      if (!consumerRunning) {
        val consumerGraph = GraphDSL.create() { implicit builder =>
          import GraphDSL.Implicits._

          val topicSet = Set(consumerTopic)
          // Kafka consumer to akka reactive stream
          val controlSource = Consumer.plainSource(consumerSettings, Subscriptions.topics(topicSet))
          // Stream processed message summary status with each consumer message received to websocket (filter data messages)
          val deserialize = Flow[ConsumerRecord[String, String]]
            .map(record => {
              logger.info(s"Consumer: topic = ${record.topic}, partition = ${record.partition}, offset = ${record.offset}, key = ${record.key} -- ${record.value}")
              val epdData = jsonToType[EpdData](record.value)
              (record.key(), PreProcessed(record.key(), epdData.uuid, epdData.CurrentTime, StreamHelper.createDataRecord(epdData)))
            })

          val lrStream = Flow[(String, PreProcessed)]
            .filter(record => record._1 == "LR")
            .map(record => {
              val key = record._1
              val value = record._2
              val ind = StreamHelper.createINDArray(value.features)
              val probs = lrModel.output(ind)
              var prediction: Int = 0
              if (probs.getDouble(1L) > probs.getDouble(0L)) {
                prediction = 1
              }
              // println(s"Key: ${value.key} -- uuid: ${value.uuid} -- recordTime: ${value.recordTime} -- Prediction: $prediction")
              val out = new ByteArrayOutputStream()
              objectMapper.writeValue(out, Result(key, value.uuid, value.recordTime, prediction))
              (key, out.toString)
            })

          val svcStream = Flow[(String, PreProcessed)]
            .filter(record => record._1 == "SVC")
            .map(record => {
              val key = record._1
              val value = record._2
              val nodes: Array[svm_node] = Array.fill(StreamHelper.numFeature)(new svm_node())
              for (col <- 0 until StreamHelper.numFeature) {
                nodes(col).index = col
                nodes(col).value = value.features(col)
              }
              val prediction = Math.round(svm.svm_predict(svmModel, nodes)).toInt
              // println(s"Key: ${value.key} -- uuid: ${value.uuid} -- recordTime: ${value.recordTime} -- Prediction: $prediction")
              val out = new ByteArrayOutputStream()
              objectMapper.writeValue(out, Result(key, value.uuid, value.recordTime, prediction))
              (key, out.toString)
            })

          val rfStream = Flow[(String, PreProcessed)]
            .filter(record => record._1 == "RF")
            .map(record => {
              val key = record._1
              val value = record._2
              val dmatrix = StreamHelper.createDMatrix(value.features)
              val output = rfModel.predict(dmatrix)
              val prediction = Math.round(output(0)(0))
              // println(s"Key: ${value.key} -- uuid: ${value.uuid} -- recordTime: ${value.recordTime} -- Prediction: $prediction")
              val out = new ByteArrayOutputStream()
              objectMapper.writeValue(out, Result(key, value.uuid, value.recordTime, prediction))
              (key, out.toString)
            })

          val gbtStream = Flow[(String, PreProcessed)]
            .filter(record => record._1 == "GBT")
            .map(record => {
              val key = record._1
              val value = record._2
              val dmatrix = StreamHelper.createDMatrix(value.features)
              val output = gbtModel.predict(dmatrix)
              val prediction = Math.round(output(0)(0))
              // println(s"Key: ${value.key} -- uuid: ${value.uuid} -- recordTime: ${value.recordTime} -- Prediction: $prediction")
              val out = new ByteArrayOutputStream()
              objectMapper.writeValue(out, Result(key, value.uuid, value.recordTime, prediction))
              (key, out.toString)
            })

          // Stream messages to kafka producer
          val dataToProducer = Flow[(String, String)]
            .map(record => {
              // logger.info(s"dataToProducer --> key: ${record._1} -- value: ${record._2} -- topic: ${producerTopic}")
              producer ! PMessage(producerTopic, record._1, record._2)
            })

          // use broadcaster to branch out reactive streams to multiple sinks
          val broadcaster = builder.add(Broadcast[(String, PreProcessed)](4))
          controlSource ~> deserialize ~> broadcaster.in
          broadcaster.out(0) ~> lrStream ~> dataToProducer ~> Sink.ignore
          broadcaster.out(1) ~> svcStream ~> dataToProducer ~> Sink.ignore
          broadcaster.out(2) ~> rfStream ~> dataToProducer ~> Sink.ignore
          broadcaster.out(3) ~> gbtStream ~> dataToProducer ~> Sink.ignore
          ClosedShape
        }
        RunnableGraph.fromGraph(consumerGraph).run
        consumerRunning = true
      }
    }
    case TerminateConsumer => {
      system.terminate()
      logger.info("System terminate for Kafka Consumer Actor...")
    }
    case _ => println("KafkaConsumerActor received something unexpected... No action taken...")
  }
}

object KafkaConsumerClass {

  case class RunConsumer(consumerTopic:String, producerTopic: String)

  case object TerminateConsumer

  // case classes for record deserialization
  case class EpdData(uuid: String, CurrentTime: String, Topic: String, Categories: HashMap[String, Int], Sensors: HashMap[String, Double], Result: Int)
  // case class for preprocessed records
  case class PreProcessed(key: String, uuid: String, recordTime: String, features: Array[Float])
  // case class for ML results
  case class Result(key: String, uuid: String, recordTime: String, prediction: Int)
}