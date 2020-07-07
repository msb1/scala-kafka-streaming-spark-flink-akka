import java.io.{ByteArrayOutputStream, File}
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.math._


case class Continuous(id: Int, name: String, max: Double, min: Double, dist: String, pos1: Double, pos2: Double, neg1: Double, neg2: Double)

case class Discrete(id: Int, name: String, num: Int, levels: String, pos: String, neg: String)

case class Alert(id: Int, name: String, atype: String, warn: Double, alarm: Double, incr: Boolean)

case class EpdConfig(name: String, err: Double, success: Double, alrt: List[Alert], cont: List[Continuous], disc: List[Discrete])

case class EquipConfig(equipName: String, fileName: String, taktTime: Int, offsetDelay: Int,
                       consumerTopic: String, producerTopic: String )

case class MasterConfig(config: List[EquipConfig])

class EpdData {
  var uuid: String = _
  var currentTime: String = _
  var name: String = _
  var categories: mutable.HashMap[String, Int] = mutable.HashMap.empty[String, Int]         // category output as int
  var sensors: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
  var result: Int = _
  private val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def this(name: String) {
    this()
    this.uuid = UUID.randomUUID().toString
    this.name = name
    this.currentTime = timeFormat.format(Calendar.getInstance.getTime)
  }
}

object DataRecord {

  val logger: Logger = LoggerFactory.getLogger("DataRecord")

  def readConfigData(masterConfig: MasterConfig, filePath: String): mutable.HashMap[String, EpdConfig] = {

    var epdConfig: mutable.HashMap[String, EpdConfig] = mutable.HashMap.empty[String, EpdConfig]

    for(v <- masterConfig.config) {
      // read config files
      logger.info(s"Reading ${v.fileName} ...")
      val yamlFile = new File(filePath + v.fileName + ".yml")
      val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
      objectMapper.registerModule(DefaultScalaModule)
      epdConfig += (v.equipName -> objectMapper.readValue[EpdConfig](yamlFile, classOf[EpdConfig]))
    }
    epdConfig.foreach({ println })
    epdConfig
  }

  def readMasterConfig(equipFileName: String): MasterConfig = {
    // read equip config master file
    logger.info(s"Reading $equipFileName ...")
    val yamlFile = new File(equipFileName)

    val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val masterConfig: MasterConfig = objectMapper
      .readValue[MasterConfig](yamlFile, classOf[MasterConfig])
    println(masterConfig)
    masterConfig
  }

  def makeSimulatedRecord(config: EpdConfig): String = {
    val random = scala.util.Random
    val epd = new EpdData(config.name)

    // determine output (1 or 0) for data record
    if (random.nextFloat() < config.success) {
      epd.result = 1
    } else {
      epd.result = 0
    }

    // generate sensor (continuous) simulated data
    for (c <- config.cont) {
      // use switch to determine simulator type
      //        var output = 0.0
      val output = c.dist match {
        // pos1/neg1 are max values for uni distribution; pos2/neg2 are min values
        case "uni" if epd.result == 1 => random.nextDouble() * (c.pos1 - c.pos2) + c.pos2
        case "uni" if epd.result == 0 => random.nextDouble() * (c.neg1 - c.neg2) + c.neg2
        // pos1/neg1 is mean of Normal distribution; pos2/neg2 is std dev
        case "norm" if epd.result == 1 => random.nextGaussian() * c.pos2 + c.pos1
        case "norm" if epd.result == 0 => random.nextGaussian() * c.neg2 + c.neg1
        // pos1/neg1 is mean of Log Normal distribution; pos2/neg2 is std dev
        case "log" if epd.result == 1 =>
          val mu: Double = log(c.pos1 * c.pos1 / sqrt(c.pos1 * c.pos1 + c.pos2 * c.pos2))
          val std: Double = log(1.0 + c.pos2 * c.pos2 / c.pos1 / c.pos1)
          exp(random.nextGaussian() * std + mu)
        case "log" if epd.result == 0 =>
          val mu: Double = log(c.neg1 * c.neg1 / sqrt(c.neg1 * c.neg1 + c.neg2 * c.neg2))
          val std: Double = log(1.0 + c.neg2 * c.neg2 / c.neg1 / c.neg1)
          exp(random.nextGaussian() * std + mu)
        // default case
        case _ => -1.0
      }
      if (output != -1.0) {
        epd.sensors += (c.name -> round(1000 * output) / 1000)
      } else {
        logger.warn(s"Improper simulator type for record: ${c.name}")
      }
    }

    // generate category endpoint simulated data
    for (d <- config.disc) {
      val rv = random.nextDouble()
      var idx = 0
      // generate category level from uniform rv
      if (epd.result == 1) {
        val probs = d.pos.split(',')
        var threshold = probs(idx).trim.toDouble
        while (rv >= threshold) {
          idx += 1
          threshold += probs(idx).trim.toDouble
        }
      } else {
        val probs = d.neg.split(',')
        var threshold = probs(idx).trim.toDouble
        while (rv >= threshold) {
          idx += 1
          threshold += probs(idx).trim.toDouble
        }
      }
      epd.categories += (d.name -> idx)
    }

    // change result according to error rate
    if (random.nextDouble() < config.err) {
      if (epd.result == 1) {
        epd.result = 0
      } else {
        epd.result = 1
      }
    }

    // convert epdData record to Json string
    val out = new ByteArrayOutputStream()
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValue(out, epd)
    // logger.info("TO Producer -> " + out.toString)
    out.toString
  }
}

