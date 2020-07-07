import akka.actor.{ActorSystem, Props}


object Main {

  def main(args: Array[String]): Unit = {

    // Suppress nasty SparkLauncher log entry header
    System.setProperty("java.util.logging.SimpleFormatter.format","%5$s%6$s%n")

    //Create actors for SparkLauncher
    val configFileName: String = "scan01-34"
    val logFileName: String = "clean"
    val system = ActorSystem("SparkLauncherMLTrainer")
    val sparkLauncher = system.actorOf(Props(new SparkLaunch(configFileName, logFileName)), "sparkLauncher")
    sparkLauncher ! Launch
  }
}
