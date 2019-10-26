package com.mycompany

import org.apache.spark.{SparkConf, SparkContext}

/**
  * zip算子：Joins two RDDs by combining the i-th of either partition with each other
  * 按算子在RDD中的位置去做combine，生成新的tuple
  * 要求两个RDD的分区数是相同的，RDD中element的个数是相同的
  */
object ZipOperator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("zip operator")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 10, 3)
    val b = sc.parallelize(11 to 20, 3)

    //打印会打印出三个分区
    a.zip(b).foreach(println)

    //打印只会有一个分区
    a.zip(b).collect().foreach(println)

    sc.stop()

  }

}
