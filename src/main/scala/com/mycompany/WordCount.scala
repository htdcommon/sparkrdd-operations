package scala.com.mycompany

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark WordCount
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("Map Demo")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("words.txt")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false)
    result.foreach(println)


    sc.stop()
  }

}
