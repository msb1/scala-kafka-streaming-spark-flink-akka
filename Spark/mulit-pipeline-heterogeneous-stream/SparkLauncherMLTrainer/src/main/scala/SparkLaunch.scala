import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.{Logger, LoggerFactory}

// Actor to Launch SparkEpdTrainer
class SparkLaunch(configName: String, logName: String) extends Actor {

  val configFileName: String = configName
  val logger: Logger = LoggerFactory.getLogger("SparkLaunch")
  val system: ActorSystem = ActorSystem("SparkLauncherMLTrainer")
  val countDownLatch = new CountDownLatch(1)

  val launcher: SparkLauncher = new SparkLauncher()
    .setSparkHome("/opt/spark-3.0.0")
    .setConf(
      "spark.jars.packages",
      "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2," +
        "com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0," +
        "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.10.0," +
        "com.typesafe.akka:akka-actor_2.12:2.6.6"
    )
    .setAppResource("/home/bw/scala/Spark/SparkMLTrainer/build/libs/SparkMLTrainer.jar")
    .setMainClass("Main")
    .setMaster("local[*]")
    .setVerbose(false)
    .addAppArgs(configFileName)
    .redirectError()
    .redirectToLog("console")

  def receive: Receive = {

    case Launch =>

      val listener = new SparkAppHandle.Listener {
        override def infoChanged(handle: SparkAppHandle): Unit = {}
        override def stateChanged(handle: SparkAppHandle): Unit = {
          logger.warn(s"Spark App Id [${handle.getAppId}] State Changed. State [${handle.getState}]")
          if (handle.getState.isFinal && handle.getAppId != null) {
            logger.warn(s"Spark App Id [${handle.getAppId}] state isFinal...")
            countDownLatch.countDown()
          }
        }
      }
      val handle: SparkAppHandle = launcher.startApplication(listener)
      countDownLatch.await()
      handle.kill()
      system.terminate()
  }

}

object Launch