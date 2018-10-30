package com.dkunert.mgr

import com.dkunert.mgr.classification.algorithm._
import com.dkunert.mgr.classification.runner.HiggsClassificationRunner
import com.dkunert.mgr.datacleanup.HiggsDataCleanup
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import com.dkunert.mgr.util.ExcelUtil
import org.apache.log4j.{Level, Logger}

object ClassificationSparkApplication {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)
    val spark = SparkSessionFactory.getSparkSession("Classification app")

    var fileName = "HIGGS_1000";

    val inputFileLocation = "C:\\magisterka\\dane\\klasyfikacja\\HIGGS\\" + fileName + ".csv"
    val outputFolder = "C:\\Users\\Dawid\\Dropbox\\prywatne\\studia\\studia\\mgr\\wyniki\\"

    var iteration = 0;

    // for loop execution with a range
    for (iteration <- 1 to 5) {
      val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, false)

      val cleanedData = HiggsDataCleanup.cleanupData(rawData)
      val Array(trainingData, testData) = cleanedData.randomSplit(Array(0.8, 0.2))

      // Naive Bayes requires nonnegative feature values but found

      // LineSupportVectorMachineAlgorithm - super długo trwa

      val allAlgorithms = List(DecisionTreeClassifierAlgorithm, GradientBoostedTreeClassifierAlgorithm, LogisticRegressionAlgorithm,
       MultilayerPerceptronClassifierAlgorithm, RandomForrestClassifierAlgorithm)


      allAlgorithms.foreach(alg => {
        val result = HiggsClassificationRunner.run(alg, cleanedData, trainingData, testData, iteration)
        ExcelUtil.writeToExcel(outputFolder + fileName + "_" + alg.getClass().getSimpleName + ".xlsx",
          iteration, result)
      })
    }
  }
}
