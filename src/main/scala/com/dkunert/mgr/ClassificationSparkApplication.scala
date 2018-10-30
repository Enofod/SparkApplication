package com.dkunert.mgr

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.dkunert.mgr.classification.algorithm._
import com.dkunert.mgr.classification.runner.HiggsClassificationRunner
import com.dkunert.mgr.datacleanup.HiggsDataCleanup
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import com.dkunert.mgr.util.ExcelUtil
import org.apache.log4j.{Level, Logger}

object ClassificationSparkApplication {

  val NUMBER_OF_FEATURES = 28

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)
    val createDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-hh-mm-ss"))

    val spark = SparkSessionFactory.getSparkSession("Classification app")
    var fileName = "HIGGS_100000";

    val inputFileLocation = "C:\\magisterka\\dane\\klasyfikacja\\HIGGS\\" + fileName + ".csv"

    val outputFolder = "C:\\Users\\Dawid\\Dropbox\\prywatne\\studia\\studia\\mgr\\wyniki\\"
    var iteration = 0;

    // for loop execution with a range
    for (iteration <- 1 to 5) {

      val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, false)
      val cleanedData = HiggsDataCleanup.cleanupData(rawData)

      // Naive Bayes requires nonnegative feature values but found

      // LineSupportVectorMachineAlgorithm - super długo trwa

      val Array(trainingData, testData) = cleanedData.randomSplit(Array(0.8, 0.2))


      val allAlgorithms = List(DecisionTreeClassifierAlgorithm, GradientBoostedTreeClassifierAlgorithm, LogisticRegressionAlgorithm,
       MultilayerPerceptronClassifierAlgorithm, RandomForrestClassifierAlgorithm)
      allAlgorithms.foreach(alg => {
        val result = HiggsClassificationRunner.run(alg, cleanedData, trainingData, testData, iteration)
        ExcelUtil.writeToExcel(outputFolder + fileName + "_" + NUMBER_OF_FEATURES + "_" + alg.getClass().getSimpleName + "_" + createDate + ".xlsx",
          iteration, result)
      })
    }
  }
}
