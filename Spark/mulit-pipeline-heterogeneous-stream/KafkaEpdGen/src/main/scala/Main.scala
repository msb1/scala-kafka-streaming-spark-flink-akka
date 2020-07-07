import SimClass.StartSimulator
import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Main {

  def main(args: Array[String]) {
    // set Akka Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)

    val filePath = "/home/bw/data/equipconfig/"
    val equipFileName = "epdsim"
    val equipment: MasterConfig = DataRecord.readMasterConfig(filePath + equipFileName + ".yml")
    val epdConfig: mutable.HashMap[String, EpdConfig] = DataRecord.readConfigData(equipment, filePath)

    println("Epd Simulator Program Started...")

    // Create actors for Kafka Producer, EPD Simulator
    implicit val system: ActorSystem = ActorSystem("EpdGen")
    val kafkaProducer = system.actorOf(Props[KafkaProducerActor], "kafkaProducer")
    val gen = system.actorOf(Props(new GenActor(kafkaProducer)), "generator")
    val epd = system.actorOf(Props(new EpdSimActor(gen, equipment, epdConfig)), "epdData")

    // send epd data to Kafka Producer
    epd ! StartSimulator
  }
}
