package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._


object FileWordCount {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val lines = ssc.textFileStream("/home/hadoop/temp/")

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
