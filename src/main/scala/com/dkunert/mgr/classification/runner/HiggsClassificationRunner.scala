package com.dkunert.mgr.classification.runner

import com.dkunert.mgr.classification.algorithm.ClassificationAlgorithm
import com.dkunert.mgr.classification.pipeline.PipelineFactory
import com.dkunert.mgr.datacleanup.HiggsDataCleanup
import com.dkunert.mgr.util.BenchmarkUtil
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

object HiggsClassificationRunner {

  def run(algorithm: ClassificationAlgorithm, rawData: DataFrame): Unit = {
    val cleanedData = HiggsDataCleanup.cleanupData(rawData)

    val pipeline = PipelineFactory.getPipeline(algorithm.get(), cleanedData)

    val Array(trainingData, testData) = cleanedData.randomSplit(Array(0.7, 0.3))

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
      .setLabelCol(PipelineFactory.INDEXED_LABEL_KEY)
      .setPredictionCol(PipelineFactory.PREDICTION_KEY)
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(className + " Accuracy = " + accuracy)
    println("------------------------------------------------------------------------------------------------------------")
  }

}
