import SparkLaunch.StartMessage
import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory


object Main {

  def main(args: Array[String]): Unit = {
    // set Akka Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    // Suppress nasty SparkLauncher log entry header
    System.setProperty("java.util.logging.SimpleFormatter.format","%5$s%6$s%n")


    //Create actors for SparkLauncher
    val system = ActorSystem("SparkEpdTrainer")
    val sparkLauncher = system.actorOf(Props[SparkLaunch], "sparkLauncher")
    val starter = system.actorOf(Props(new Starter(sparkLauncher)), "starter")
    starter ! StartMessage				// start sparklauncher

  }
}
