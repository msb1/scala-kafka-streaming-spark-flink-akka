import java.io.File

import akka.actor.ActorSystem
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.HashMap

object Utils {
  // define ActorSystem and materializer for akka streams
  implicit val system: ActorSystem = ActorSystem("SparkMLTrainer")
  val logger: Logger = LoggerFactory.getLogger("Utils")

  // define case classes for training data and configuration
  case class Continuous(id: Int, name: String, max: Double, min: Double, dist: String, pos1: Double, pos2: Double, neg1: Double, neg2: Double)

  case class Discrete(id: Int, name: String, num: Int, levels: String, pos: String, neg: String)

  case class Alert(id: Int, name: String, atype: String, warn: Double, alarm: Double, incr: Boolean)

  case class EquipConfig(name: String, err: Double, success: Double, alrt: Array[Alert], cont: Array[Continuous], disc: Array[Discrete])

  case class EpdData(CurrentTime: String, Topic: String, Categories: HashMap[String, Int], Sensors: HashMap[String, Double], Result: Int)

  val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  // read in equip config from file
  def readConfigData(configFileName: String): EquipConfig = {
    logger.info(s"Reading $configFileName ...")
    val yamlFile = new File(configFileName)
    val epdConfig: EquipConfig = objectMapper.readValue[EquipConfig](yamlFile, classOf[EquipConfig])
    println(epdConfig)
    epdConfig
  }

}
