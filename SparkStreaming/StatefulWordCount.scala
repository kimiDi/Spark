package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object StatefulWordCount {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: StatefulWordCount <filename> <port> ")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // define method of updating state, values: the current word count, state: past word count
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // create StreamingContext, set Spark Steaming runtime interval to 5s
    val ssc = new StreamingContext(sc, Seconds(5))
    // set checkpoint
    ssc.checkpoint(".")

    // Receive data from socket
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1))

    // update key and calculate the total count from the beginning
    val stateDstream = wordCounts.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
