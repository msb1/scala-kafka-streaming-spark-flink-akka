package brokers

import actors.KafkaConsumerClass.EpdData
import ml.dmlc.xgboost4j.scala.DMatrix
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.immutable.HashMap

object StreamHelper {

  // category and sensor levels
  val categoryLevel: HashMap[String, Int] = HashMap("cat0" -> 2, "cat1" -> 5, "cat2" -> 4, "cat3" -> 3, "cat4" -> 2,
    "cat5" -> 5, "cat6" -> 4, "cat7" -> 3)
  val sensorLevel: HashMap[String, Float] = HashMap("sensor0" -> 25F, "sensor1" -> 50F, "sensor2" -> 75F, "sensor3" -> 100F,
    "sensor4" -> 125F, "sensor5" -> 150F, "sensor6" -> 175F,
    "sensor7" -> 200F, "sensor8" -> 225F, "sensor9" -> 250F)

  val startingCatIndex = Array(10, 11, 15, 18, 20, 21, 25, 28)

  val numFeature = 30
  val numClass = 2

  /* Convert data to libsvm format and write to file
   *  Indexes:
   *    0 -> sensor0, 1 -> sensor1, 2 -> sensor2, 3 -> sensor3, 4 -> sensor4,
   *    5 -> sensor5, 6 -> sensor6, 7 -> sensor7, 8 -> sensor8, 9 -> sensor9,
   *    10 -> cat0, (11, 12, 13, 14) -> cat1, (15, 16, 17) -> cat2, (18, 19) -> cat3, 20 -> cat4,
   *    (21, 22, 23, 24) -> cat5, (25, 26, 27) - > cat6, (28, 29) -> cat7
   */

  def createDataRecord(record: EpdData): Array[Float] = {
    val features: Array[Float] = new Array[Float](numFeature)
    // add sensor data with specified indexes
    for (i <- 0 to 9) {
      features(i) = record.Sensors("sensor" + i).toFloat / sensorLevel("sensor" + i)
    }
    // add one hot categories with specified indexes
    for (i <- 0 to 7) {
      for (j <- startingCatIndex(i) until startingCatIndex(i) + categoryLevel("cat" + i) - 1) {
        val index = j - startingCatIndex(i) + 1
        if (index == record.Categories("cat" + i)) {
          features(j) = 1
        }
      }
    }
    features
  }

  def createINDArray(features: Array[Float]): INDArray = {
    val numRows: Long = 1L
    val indArray = Nd4j.zeros(numRows, numFeature)
    // add sensor data with specified indexes
    for (i <- 0 to 9) {
      indArray.putScalar(0, i, features(i))
    }
    // add one hot categories with specified indexes
    for (i <- 0 to 7) {
      for (j <- startingCatIndex(i) until startingCatIndex(i) + categoryLevel("cat" + i) - 1) {
        val index = j - startingCatIndex(i) + 1
        indArray.putScalar(0, j, features(j))
      }
    }
    indArray
  }

  def createDMatrix(features: Array[Float]): DMatrix = {
    val dmatrix: DMatrix = new DMatrix(features, 1, numFeature)
    dmatrix
  }
}
