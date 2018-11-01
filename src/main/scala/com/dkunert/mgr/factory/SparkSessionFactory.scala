package com.dkunert.mgr.factory

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def getSparkSession(applicationName: String): SparkSession = {
    return SparkSession.builder()
      .appName(applicationName)
      .config("spark.master", "local")
      .config("spark.local.dir", "C:\\spark-cache")
      .getOrCreate();
  }
}
