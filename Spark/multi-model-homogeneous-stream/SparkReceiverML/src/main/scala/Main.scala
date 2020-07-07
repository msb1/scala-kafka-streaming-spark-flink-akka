import Broker.SparkMLConsumer

object Main {

  def main(args: Array[String]) {
    val bootstrapServer = "192.168.21.10:9092"
    val ptopic = "epd01"
    val ctopic = "spark01"

    // Spark SQL Stream - data records from Kafka Consumer
    SparkMLConsumer.runSparkConsumer(ctopic, ptopic, bootstrapServer)
  }
}
