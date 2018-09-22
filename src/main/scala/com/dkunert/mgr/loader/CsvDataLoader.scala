package com.dkunert.mgr.loader

import org.apache.spark.sql.{DataFrame, SparkSession}


object CsvDataLoader {

  def loadCsvData(sparkSession: SparkSession, location: String, containsHeaderLine: Boolean): DataFrame = {
    return sparkSession.read
      .format("csv")
      .option("header", containsHeaderLine)
      .load(location)
  }
}
