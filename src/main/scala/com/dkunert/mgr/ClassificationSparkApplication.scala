package com.dkunert.mgr

import com.dkunert.mgr.classification.algorithm._
import com.dkunert.mgr.classification.runner.HiggsClassificationRunner
import com.dkunert.mgr.factory.SparkSessionFactory
import com.dkunert.mgr.loader.CsvDataLoader
import org.apache.log4j.{Level, Logger}

object ClassificationSparkApplication {
  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)
    val spark = SparkSessionFactory.getSparkSession("Classification app")

    //val inputFileLocation = "F:\\bigdata\\titanic\\all\\train.csv"
    //val inputFileLocation = "C:\\magisterka\\dane\\klasyfikacja\\poker.csv"
    //val inputFileLocation = "C:\\magisterka\\dane\\klasyfikacja\\adult.csv"
    val inputFileLocation = "C:\\magisterka\\dane\\klasyfikacja\\HIGGS\\HIGGS_1000.csv"

    val rawData = CsvDataLoader.loadCsvData(spark, inputFileLocation, false)

    //MultinominalLogisticRegressionAlgorithm.run(transformedData)
    //DecisionTreeClassifierAlgorithm.run(transformedData)
    //RandomForrestClassifierAlgorithm.run(transformedData)
    //GradientBoostedTreeClassifierAlgorithm.run(transformedData) // ONLY FOR BINARY
    //MultilayerPerceptronClassifierAlgorithm.run(transformedData)
    //LineSupportVectorMachineAlgorithm.run(transformedData) // ONLY FOR BINARY

    // Naive Bayes requires nonnegative feature values but found

    val allAlgorithms = List(DecisionTreeClassifierAlgorithm, GradientBoostedTreeClassifierAlgorithm, LineSupportVectorMachineAlgorithm, MultinominalLogisticRegressionAlgorithm,
      PerceptronClassifierAlgorithm, RandomForrestClassifierAlgorithm)

    allAlgorithms.foreach(alg => HiggsClassificationRunner.run(alg, rawData))

    //HiggsClassificationRunner.run(MultinominalLogisticRegressionAlgorithm, rawData)

  }
}
