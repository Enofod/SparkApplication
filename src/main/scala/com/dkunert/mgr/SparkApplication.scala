package com.dkunert.mgr

import com.dkunert.mgr.datacleanup.TitanicDataCleanup
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader

object SparkApplication {
  def main(args:Array[String]): Unit = {
    val spark = SparkSessionFactory.getSparkSession("Titanic app")

    val inputFileLocation = "F:\\bigdata\\titanic\\all\\train.csv"

    val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, true)

    val transformedData = TitanicDataCleanup.cleanupData(rawData)

    transformedData.head(5).foreach(println)

    //KMeansAlgorithm.run(transformedData)
    //LDAAlgorithm.run(transformedData)
    //BisectingKMeansAlgorithm.run(transformedData)
    //GMMAlgorithm.run(transformedData)
  }
}
