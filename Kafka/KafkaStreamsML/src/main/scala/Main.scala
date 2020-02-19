import actors.KafkaConsumerActor
import actors.KafkaConsumerClass.RunConsumer
import akka.actor.{ActorSystem, Props}
import broker.KafkaProducerActor
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {

    // set Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)
    val logger = LoggerFactory.getLogger("Main")

    logger.info("Kafka Akka Streams ML program...")
    val system = ActorSystem("KafkaStreamsML")

    // Create actors for Kafka Producer and Consumer
    val kafkaProducer = system.actorOf(Props[KafkaProducerActor], "kafkaProducer")
    val kafkaConsumer = system.actorOf(Props(new KafkaConsumerActor(kafkaProducer)), "kafkaConsumer")

    // start Kafka Consumer and Akka ML Stream
    val consumerTopic = "str01"
    val producerTopic = "epd01"
    kafkaConsumer ! RunConsumer(consumerTopic, producerTopic)

  }
}