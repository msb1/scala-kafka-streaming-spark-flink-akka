import broker.KafkaProducerActor
import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.{Level, Logger}
import generator.DataRecord
import generator.DataRecord.EpdSimActor
import generator.DataRecord.SimClass.{InitSimulator, StartSimulator}
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {
    // set Akka Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)

    val topic = "data01"
    val fileName = "/home/bw/scala/Misc/kafkaTest/epd.json"
    val epdConfig = DataRecord.readConfigData(fileName)

    println("Kafka Epd Generator Program...")

    // Create actors for Kafka Producer, EPD Simulator
    val system = ActorSystem("EpdGen")
    val kafkaProducer = system.actorOf(Props[KafkaProducerActor], "kafkaProducer")
    val epdData = system.actorOf(Props(new EpdSimActor(kafkaProducer, epdConfig, topic)), "epdData")

    // send epd data to Kafka Producer
    epdData ! InitSimulator
    epdData ! StartSimulator
  }
}

