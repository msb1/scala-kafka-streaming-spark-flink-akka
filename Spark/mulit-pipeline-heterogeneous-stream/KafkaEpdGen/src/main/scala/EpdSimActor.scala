import GenClass.{GenRecord, Terminate}
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Terminated}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

// Actor class for EPD simulator
class EpdSimActor(genRecord: ActorRef, masterConfig: MasterConfig, epdConfig: mutable.HashMap[String, EpdConfig])
      extends Actor {

  import SimClass._

  val logger: Logger = LoggerFactory.getLogger("EpdSimActor")
  implicit val system: ActorSystem = ActorSystem("EpdGen")
  val cancellableListBuffer: ListBuffer[Cancellable] = ListBuffer.empty[Cancellable]

  def receive: Receive = {
    case StartSimulator =>
      cancellableListBuffer.clear()
      import system.dispatcher
        logger.debug("Inside StartSimulator")
        //genRecord !  GenRecord("hack", "hack", epdConfig("hack"))
      for (v <- masterConfig.config) {
        cancellableListBuffer += system.scheduler.scheduleWithFixedDelay(
          DurationInt(v.offsetDelay).milliseconds,
          DurationInt(v.taktTime).milliseconds,
          genRecord,
          GenRecord(v.producerTopic, v.equipName, epdConfig(v.equipName)))
      }

    case Terminated =>
      for (cancellable: Cancellable <- cancellableListBuffer) {
        cancellable.cancel()
      }
      genRecord ! Terminate
      logger.info("EPD Simulator stopped...")
      System.exit(0)

    case _ => logger.info("EPD Simulator Actor received something unexpected...")
  }
}

object SimClass {

  case object StartSimulator

}




