import KafkaProducerClass.PMessage
import akka.actor.{Actor, ActorRef, ActorSystem}
import org.slf4j.{Logger, LoggerFactory}

class GenActor(producer: ActorRef) extends Actor {

  import GenClass._
  val logger: Logger = LoggerFactory.getLogger("EpdSimActor")
  implicit val system: ActorSystem = ActorSystem("EpdGen")

  override def receive: Receive = {

    case GenRecord(topic, key, epdConfig) =>
      val value = DataRecord.makeSimulatedRecord(epdConfig)
      // logger.info(s"GenRecord: topic=$topic, key=$key, value=$value")
      producer ! PMessage(topic, key, value)

    case Terminate =>
      producer ! Terminate
      logger.info("System terminate for Record Generator and Kafka Producer...")

    case _ => logger.info("Record generator received something unexpected... No action taken...")
  }
}

object GenClass {

  case class GenRecord(topic: String, key: String, epdConfig: EpdConfig)

  case object Terminate

}