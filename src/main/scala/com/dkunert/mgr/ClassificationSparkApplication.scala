package com.dkunert.mgr

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZonedDateTime}

import com.dkunert.mgr.classification.algorithm._
import com.dkunert.mgr.classification.runner.HiggsClassificationRunner
import com.dkunert.mgr.datacleanup.{HiggsDataCleanup, HiggsMinimalDataCleanup}
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import com.dkunert.mgr.util.ExcelUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

object ClassificationSparkApplication {

  var NUMBER_OF_FEATURES = 28

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)
    val createDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-hh-mm-ss"))

    val spark = SparkSessionFactory.getSparkSession("Classification app")

    var fileNames = List("HIGGS_1000", "HIGGS_10000", "HIGGS_100000", "HIGGS_1000000", "HIGGS_4000000", "HIGGS_8000000", "HIGGS")

    val outputFolder = "C:\\Users\\Dawid\\Dropbox\\prywatne\\studia\\studia\\mgr\\wyniki\\nowe\\"
    var iteration = 0;

    fileNames.foreach(fileName => {
      val inputFileLocation = "E:\\magisterka\\dane\\klasyfikacja\\HIGGS\\" + fileName + ".csv"
      NUMBER_OF_FEATURES = 28

      // for loop execution with a range
      for (iteration <- 1 to 10) {
        var rowCount = iteration
        if (iteration == 6)
          NUMBER_OF_FEATURES = 7

        val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, false)

        var cleanedData: DataFrame = null
        if (iteration <= 5) {
          cleanedData = HiggsDataCleanup.cleanupData(rawData)
        } else {
          rowCount = iteration - 5
          cleanedData = HiggsMinimalDataCleanup.cleanupData(rawData)
        }

        val Array(trainingData, testData) = cleanedData.randomSplit(Array(0.8, 0.2))


        val allAlgorithms = List(GradientBoostedTreeClassifierAlgorithm, DecisionTreeClassifierAlgorithm, LogisticRegressionAlgorithm,
          MultilayerPerceptronClassifierAlgorithm, RandomForrestClassifierAlgorithm)
        allAlgorithms.foreach(alg => {
          val result = HiggsClassificationRunner.run(alg, cleanedData, trainingData, testData, rowCount)
          ExcelUtil.writeToExcel(outputFolder + fileName + "_" + NUMBER_OF_FEATURES + "_" + alg.getClass().getSimpleName + "_" + createDate + ".xlsx",
            rowCount, result)
        })
      }
    })

    println("END DATE: " + ZonedDateTime.now())
  }

}
