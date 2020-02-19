import java.io.{BufferedWriter, File, FileWriter}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import ch.qos.logback.classic.{Level, Logger}
import com.mongodb.reactivestreams.client.MongoClients
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Utils {

  // set Logger levels to WARN (to avoid excess verbosity)
  LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
  LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)

  // define ActorSystem and Materializer for akka streams
  implicit val actorSystem = ActorSystem("FlinkMLTrainer")
  implicit val materializer = ActorMaterializer()

  case class EpdData(CurrentTime: String, Topic: String, Categories: HashMap[String, Int], Sensors: HashMap[String, Double], Result: Int)


  // category and sensor levels
  val categoryLevel: HashMap[String, Int] = HashMap("cat0" -> 2, "cat1" -> 5, "cat2" -> 4, "cat3" -> 3, "cat4" -> 2,
    "cat5" -> 5, "cat6" -> 4, "cat7" -> 3)
  val sensorLevel: HashMap[String, Double] = HashMap("sensor0" -> 25, "sensor1" -> 50, "sensor2" -> 75, "sensor3" -> 100,
    "sensor4" -> 125, "sensor5" -> 150, "sensor6" -> 175,
    "sensor7" -> 200, "sensor8" -> 225, "sensor9" -> 250)

  val startingCatIndex = Array(10, 11, 15, 18, 20, 21, 25, 28)

  /* Convert data to libsvm format and write to file
   *  Indexes:
   *    0 -> sensor0, 1 -> sensor1, 2 -> sensor2, 3 -> sensor3, 4 -> sensor4,
   *    5 -> sensor5, 6 -> sensor6, 7 -> sensor7, 8 -> sensor8, 9 -> sensor9,
   *    10 -> cat0, (11, 12, 13, 14) -> cat1, (15, 16, 17) -> cat2, (18, 19) -> cat3, 20 -> cat4,
   *    (21, 22, 23, 24) -> cat5, (25, 26, 27) - > cat6, (28, 29) -> cat7
   */

  def createEpdDataFiles(): Array[EpdData] = {

    // Read in Mongo DB Data
    val codecRegistry = fromRegistries(fromProviders(classOf[EpdData]), DEFAULT_CODEC_REGISTRY)
    val client = MongoClients.create("mongodb://barnwaldo:shakeydog@192.168.21.10:27017/?authSource=admin")
    val db = client.getDatabase("barnwaldo")
    val epd01 = db.getCollection("epd01", classOf[EpdData]).withCodecRegistry(codecRegistry)
    val source: Source[EpdData, NotUsed] = MongoSource(epd01.find(classOf[EpdData]))
    val result: Future[Seq[EpdData]] = source.runWith(Sink.seq)
    implicit val timeout = Timeout(5 seconds)
    val epdData = Await.result(result, timeout.duration).asInstanceOf[Seq[EpdData]]
    println("Number of records read from edp01 MongoDB: " + epdData.length.toString)
//    for (i <- 0 to 5) {
//      println(s"${epdData(i).CurrentTime}  ${epdData(i).Topic}  ${epdData(i).Result} ")
//    }

    // convert epdData to libsvm and csv formats and write to train/test files
    val trainFile = new File("epdData.train.svm.txt")
    val bwTrain = new BufferedWriter(new FileWriter(trainFile))
    val testFile = new File("epdData.test.svm.txt")
    val bwTest = new BufferedWriter(new FileWriter(testFile))

    val csvTrainFile = new File("epdDataTrain.csv")
    val bwTrainCsv = new BufferedWriter(new FileWriter(csvTrainFile))
    val csvTestFile = new File("epdDataTest.csv")
    val bwTestCsv = new BufferedWriter(new FileWriter(csvTestFile))

    val testIndex = 5

    for (k <- 0 until epdData.length) {
      val record = epdData(k)
      val line = new StringBuilder
      val csvLine = new StringBuilder
      // label is first entry
      line ++= record.Result.toString
      line += ' '
      // add sensor data with specified indexes
      for (i <- 0 to 9) {
        val normalizedSensor = record.Sensors("sensor" + i) / sensorLevel("sensor" + i)
        line ++= f"${i.toString}:$normalizedSensor%.3f "
        csvLine ++= f"$normalizedSensor%.3f, "
      }
      // add one hot categories with specified indexes
      for (i <- 0 to 7) {
        for (j <- startingCatIndex(i) until startingCatIndex(i) + categoryLevel("cat" + i) - 1) {
          val index = j - startingCatIndex(i) + 1
          if (index == record.Categories("cat" + i)) {
            line ++= s"${j.toString}:1 "
            csvLine ++= "1, "
          } else {
            line ++= s"${j.toString}:0 "
            csvLine ++= "0, "
          }
        }
      }
      csvLine ++= record.Result.toString
      if (k % testIndex == 0) {
        bwTest.write(line.toString() + '\n')
        bwTestCsv.write(csvLine.toString() + '\n')
      } else {
        bwTrain.write(line.toString() + '\n')
        bwTrainCsv.write(csvLine.toString() + '\n')
      }
      // println(line.toString())
    }
    bwTrain.close()
    bwTest.close()
    bwTrainCsv.close()
    bwTestCsv.close()
    epdData.toArray
  }

  def createINDArray(record: EpdData, numFeatures: Long): INDArray = {
    val numRows: Long = 1L
    val indArray = Nd4j.zeros(numRows, numFeatures)
    // add sensor data with specified indexes
    for (i <- 0 to 9) {
      val normalizedSensor = record.Sensors("sensor" + i) / sensorLevel("sensor" + i)
      indArray.putScalar(0, i, normalizedSensor)
    }
    // add one hot categories with specified indexes
    for (i <- 0 to 7) {
      for (j <- startingCatIndex(i) until startingCatIndex(i) + categoryLevel("cat" + i) - 1) {
        val index = j - startingCatIndex(i) + 1
        if (index == record.Categories("cat" + i)) {
          indArray.putScalar(0, j, 1)
        } else {
          indArray.putScalar(0, j, 0)
        }
      }
    }
    indArray
  }
}
