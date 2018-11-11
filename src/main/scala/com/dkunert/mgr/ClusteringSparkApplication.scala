package com.dkunert.mgr

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZonedDateTime}

import com.dkunert.mgr.classification.runner.HiggsClusteringRunner
import com.dkunert.mgr.clustering.algorithm.{BisectingKMeansAlgorithm, GMMAlgorithm, KMeansAlgorithm}
import com.dkunert.mgr.datacleanup.clustering.{HiggsClusteringDataCleanup, HiggsClusteringMinimalDataCleanup}
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import com.dkunert.mgr.util.ExcelClusteringUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

object ClusteringSparkApplication {

  var NUMBER_OF_FEATURES = 28

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)
    val createDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-hh-mm-ss"))

    val spark = SparkSessionFactory.getSparkSession("Clustering app")

    var fileNames = List("HIGGS_1000", "HIGGS_10000", "HIGGS_100000", "HIGGS_1000000", "HIGGS_4000000", "HIGGS_8000000", "HIGGS")
    //var fileNames = List("HIGGS_8000000", "HIGGS")


    val outputFolder = "C:\\Users\\Dawid\\Dropbox\\prywatne\\studia\\studia\\mgr\\wyniki\\klasteryzacja\\"
    var iteration = 0;

    fileNames.foreach(fileName => {
      val inputFileLocation = "E:\\magisterka\\dane\\klasyfikacja\\HIGGS\\" + fileName + ".csv"
      NUMBER_OF_FEATURES = 28

      // for loop execution with a range
      for (iteration <- 1 to 6) {
        var rowCount = iteration
        if (iteration == 3)
          NUMBER_OF_FEATURES = 7

        val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, false)

        var cleanedData: DataFrame = null
        if (iteration <= 3) {
          cleanedData = HiggsClusteringDataCleanup.cleanupData(rawData)
        } else {
          rowCount = iteration - 3
          cleanedData = HiggsClusteringMinimalDataCleanup.cleanupData(rawData)
        }

        // LDA TAKES WAY TO LONG TIME
        val allAlgorithms = List(BisectingKMeansAlgorithm, GMMAlgorithm, KMeansAlgorithm)
        allAlgorithms.foreach(alg => {
          val result = HiggsClusteringRunner.run(alg, cleanedData, rowCount)
          ExcelClusteringUtil.writeToExcel(outputFolder + fileName + "_" + NUMBER_OF_FEATURES + "_" + alg.getClass().getSimpleName + "_" + createDate + ".xlsx",
            rowCount, result)
        })
      }
    })

    println("END DATE: " + ZonedDateTime.now())
  }

}
