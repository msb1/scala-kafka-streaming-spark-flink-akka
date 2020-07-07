package Utils

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

object FileUtils {
  // define ActorSystem and materializer for akka streams
  val logger: Logger = LoggerFactory.getLogger("Utils")

  // define case classes for training data and configuration
  case class Continuous(id: Int, name: String, max: Double, min: Double, dist: String, pos1: Double, pos2: Double, neg1: Double, neg2: Double)

  case class Discrete(id: Int, name: String, num: Int, levels: String, pos: String, neg: String)

  case class Alert(id: Int, name: String, atype: String, warn: Double, alarm: Double, incr: Boolean)

  case class EpdConfig(name: String, err: Double, success: Double, alrt: List[Alert], cont: List[Continuous], disc: List[Discrete])

  case class EquipConfig(equipName: String, fileName: String, taktTime: Int, offsetDelay: Int,
                         consumerTopic: String, producerTopic: String )

  case class MasterConfig(config: List[EquipConfig])

  case class EpdData(uuid: String, currentTime: String, topic: String,
                     categories: HashMap[String, Int], sensors: HashMap[String, Double], result: Int)


  val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def readConfigData(masterConfig: MasterConfig, filePath: String): List[EpdConfig] = {

    val equipConfig: List[EquipConfig] = masterConfig.config
    var epdConfig: ListBuffer[EpdConfig] = ListBuffer.empty[EpdConfig]

    for(equip <- equipConfig) {
      // read config files
      logger.info(s"Reading ${equip.fileName} ...")
      val yamlFile = new File(filePath + equip.fileName + ".yml")
      val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
      objectMapper.registerModule(DefaultScalaModule)
      epdConfig += objectMapper.readValue[EpdConfig](yamlFile, classOf[EpdConfig])
    }
    epdConfig.foreach(epd => logger.info(epd.toString))
    epdConfig.toList
  }

  def readMasterConfig(equipFileName: String): MasterConfig = {
    // read equip config master file
    logger.info(s"Reading $equipFileName ...")
    val yamlFile = new File(equipFileName)

    val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val masterConfig: MasterConfig = objectMapper
      .readValue[MasterConfig](yamlFile, classOf[MasterConfig])
    logger.info(masterConfig.toString)
    masterConfig
  }

  def readKafkaConfig(kafkaConfigFileName: String): HashMap[String, String] = {
    logger.info(s"Reading $kafkaConfigFileName ...")
    val yamlFile = new File(kafkaConfigFileName)
    val kafkaConfig: HashMap[String, String] = objectMapper.readValue[HashMap[String, String]](yamlFile, classOf[HashMap[String, String]])
    logger.info(kafkaConfig.toString)
    kafkaConfig
  }
}
