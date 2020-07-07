import Utils.FileUtils
import Utils.FileUtils.{EpdConfig, MasterConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.HashMap

object Main {

  val logger: Logger = LoggerFactory.getLogger("Main")

  def main(args: Array[String]) {

    val equipFilePath = "/home/bw/data/equipconfig/"
    val sparkmlFilePath = "/home/bw/data/sparkml/"

    val kafkaConfig: HashMap[String, String] = FileUtils.readKafkaConfig(sparkmlFilePath + "spark-kafka" + ".yml")

    val equipment: MasterConfig = FileUtils.readMasterConfig(equipFilePath + "epdsim" + ".yml")
    val epdConfig: Array[EpdConfig] = FileUtils.readConfigData(equipment, equipFilePath).toArray

    val producerTopic: String = kafkaConfig("producer-topic")
    logger.info("Producer: " + producerTopic)
    val consumerTopic: String = kafkaConfig("consumer-topic")
    logger.info("Consumer: " + consumerTopic)
    val bootstrapServer = kafkaConfig("bootstrap-server")
    logger.info("Bootstrap-Server: " + bootstrapServer)

    // Create actors for Kafka Producer, EPD Simulator
    SparkReceiver.process(epdConfig, producerTopic, consumerTopic, bootstrapServer, sparkmlFilePath)

  }
}
