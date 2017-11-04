package SparkML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors


object LinearRegression {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("LinearRegression").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("/home/hadoop/upload/lpsa.data")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    // Building the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.reduce (_ + _) / valuesAndPreds.count
    println("training Mean Squared Error = " + MSE)
    sc.stop()
  }
}
