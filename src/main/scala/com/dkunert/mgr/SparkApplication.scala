package com.dkunert.mgr

import com.dkunert.mgr.clustering.algorithm.{BisectingKMeansAlgorithm, GMMAlgorithm, KMeansAlgorithm, LDAAlgorithm}
import com.dkunert.mgr.datacleanup.UsCensusDataCleanup
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import com.dkunert.mgr.util.BenchmarkUtil

object SparkApplication {
  def main(args:Array[String]): Unit = {
    val spark = SparkSessionFactory.getSparkSession("Titanic app")

    //val inputFileLocation = "F:\\bigdata\\titanic\\all\\train.csv"
    val inputFileLocation = "C:\\magisterka\\dane\\USCensus1990.csv"

    val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, true)

    //val transformedData = TitanicDataCleanup.cleanupData(rawData)
    val transformedData = UsCensusDataCleanup.cleanupData(rawData)

    transformedData.head(5).foreach(println)

    BenchmarkUtil.startTime();
    println("KMEANS")
    KMeansAlgorithm.run(transformedData)
    println("LDA")
    LDAAlgorithm.run(transformedData)
    println("BISECT KMEANS")
    BisectingKMeansAlgorithm.run(transformedData)
    println("GMA")
    GMMAlgorithm.run(transformedData)

    println("Processing time: " + BenchmarkUtil.getProcessingTime())
  }
}
