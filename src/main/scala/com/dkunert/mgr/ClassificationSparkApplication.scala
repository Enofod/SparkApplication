package com.dkunert.mgr

import com.dkunert.mgr.classification.algorithm.MultinominalLogisticRegressionAlgorithm
import com.dkunert.mgr.datacleanup.PokerDataCleanup
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import com.dkunert.mgr.util.BenchmarkUtil

object ClassificationSparkApplication {
  def main(args:Array[String]): Unit = {
    val spark = SparkSessionFactory.getSparkSession("Classification app")

    //val inputFileLocation = "F:\\bigdata\\titanic\\all\\train.csv"
    val inputFileLocation = "C:\\magisterka\\dane\\klasyfikacja\\poker_1000.csv"

    val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, false)

    val transformedData = PokerDataCleanup.cleanupData(rawData)

    //transformedData.head(5).foreach(println)

    BenchmarkUtil.startTime();

    MultinominalLogisticRegressionAlgorithm.run(transformedData)
    println("Processing time: " + BenchmarkUtil.getProcessingTime())
  }
}
