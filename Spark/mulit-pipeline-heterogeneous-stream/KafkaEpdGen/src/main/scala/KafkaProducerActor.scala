import akka.actor.{Actor, ActorSystem}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

class KafkaProducerActor extends Actor {

  import KafkaProducerClass._

  // define ActorSystem and Materializer for akka streams
  implicit val system: ActorSystem = ActorSystem("EpdGen")
  val logger: Logger = LoggerFactory.getLogger("KafkaProducerActor")

  // kafka producer config/settings
  val proConfig: Config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, String] = ProducerSettings(proConfig, new StringSerializer, new StringSerializer)

  def receive: Receive = {

    case PMessage(topic, key, value) =>
      Source.single(PMessage(topic, key, value))
        .map(msg => {
          logger.info(s"key: ${msg.key} -- value: ${msg.value} -- topic: ${msg.topic}")
          new ProducerRecord(msg.topic, msg.key, msg.value)
        })
        .runWith(Producer.plainSink(producerSettings))
        //.runWith(Sink.ignore)       // for testing without Kafka

    case Terminate =>
      system.terminate()
      logger.info("System terminate for Kafka Producer...")

    case _ => logger.info("KafkaProducer received something unexpected... No action taken...")
  }
}

object KafkaProducerClass {

  case class PMessage(topic: String, key: String, value: String)

  case object Terminate

}