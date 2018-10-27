package com.dkunert.mgr.classification.runner

import com.dkunert.mgr.classification.algorithm.ClassificationAlgorithm
import com.dkunert.mgr.classification.pipeline.PipelineFactory
import com.dkunert.mgr.util.BenchmarkUtil
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

object HiggsClassificationRunner {

  def run(algorithm: ClassificationAlgorithm, cleanedData: DataFrame, trainingData: DataFrame, testData: DataFrame): Unit = {
    val pipeline = PipelineFactory.getPipeline(algorithm.get(), cleanedData)

    /*println("cleaned")
    cleanedData.printSchema()
    cleanedData.head(5).foreach(println)
    println("training")
    trainingData.printSchema()
    trainingData.head(5).foreach(println)
    println("test")
    testData.printSchema()
    testData.head(5).foreach(println)*/


    // Train model.
    val className = algorithm.getClass().getSimpleName();
    BenchmarkUtil.startTime()
    val trainModel = pipeline.fit(trainingData)
    println(className + " Train model time [ms]: " + BenchmarkUtil.getProcessingTime())

    // Make predictions.
    BenchmarkUtil.startTime()
    val predictions = trainModel.transform(testData)
    println(className + " Predict on model time [ms]: " + BenchmarkUtil.getProcessingTime())

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(PipelineFactory.LABEL_KEY)
      .setPredictionCol(PipelineFactory.PREDICTION_KEY)
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(className + " Accuracy = " + accuracy)
    println("------------------------------------------------------------------------------------------------------------")
  }

}
