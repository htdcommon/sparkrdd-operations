package com.mycompany

import org.apache.spark.{SparkConf, SparkContext}

/**
  * cogroup:将多个RDD中的同一个key对应的不同的value组合到一起
  *
  */
object CogroupOperator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("cogroup operator")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.map((_, "b"))
    val c = a.map((_, "c"))

    b.cogroup(c).foreach(println)
    sc.stop()

  }

}
