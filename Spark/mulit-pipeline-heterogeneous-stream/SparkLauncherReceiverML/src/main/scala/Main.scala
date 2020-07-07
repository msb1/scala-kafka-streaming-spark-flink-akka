import akka.actor.{ActorSystem, Props}


object Main {

  def main(args: Array[String]): Unit = {

    // Suppress nasty SparkLauncher log entry header
    System.setProperty("java.util.logging.SimpleFormatter.format","%5$s%6$s%n")

    //Create actors for SparkLauncher
    val configString: String = "epdsim"
    val system = ActorSystem("SparkLauncherMLTrainer")
    val sparkLauncher = system.actorOf(Props[SparkLaunch], "sparkLauncher")
    sparkLauncher ! Launch
  }
}
