package com.dkunert.mgr

import org.apache.spark.{SparkConf, SparkContext}

object SparkApplication {
  def main(args:Array[String]): Unit = {
    val sparkConfig = new SparkConf()
    sparkConfig.setMaster("local")
    sparkConfig.setAppName("First application")

    val sparkContext = new SparkContext(sparkConfig)

    val rdd1 = sparkContext.makeRDD(Array(1,2,3,4,5,6))
    rdd1.collect().foreach(println)
  }
}
