package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object WindowWordCount {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: WindowWorldCount <filename> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // create StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    // set checkpoint
    ssc.checkpoint(".")

    // Receive data from socket
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(","))

    val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(args(2).toInt), Seconds(args(3).toInt))
    //val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow(_+_, _-_,Seconds(args(2).toInt), Seconds(args(3).toInt))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
