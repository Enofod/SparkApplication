package com.dkunert.mgr.classification.runner

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import com.dkunert.mgr.clustering.algorithm.ClusteringAlgorithm
import com.dkunert.mgr.util.BenchmarkUtil
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.DataFrame

object HiggsClusteringRunner {

  def run(algorithm: ClusteringAlgorithm, cleanedData: DataFrame, trainingData: DataFrame, testData: DataFrame, iteration: Double): List[Double] = {
    val pipeline = PipelineFactory.getPipeline(algorithm.get(), cleanedData)
    println("START: " + algorithm.getClass.getSimpleName + " iteration:" + iteration + " ")

    // Train model.
    val className = algorithm.getClass().getSimpleName();
    BenchmarkUtil.startTime()
    val trainModel = pipeline.fit(trainingData)
    val trainTime = BenchmarkUtil.getProcessingTime()
    println(className + " Train time [ms]: " + trainTime)

    // Make predictions.
    BenchmarkUtil.startTime()
    val predictions = trainModel.transform(testData)
    predictions.collect()
    System.gc()
    val testTime = BenchmarkUtil.getProcessingTime()
    println(className + " Prediction time [ms]: " + testTime)

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println("Silhouette BisectingKMeansAlgorithm: ", silhouette)


    return List(iteration, trainTime, testTime, silhouette)
  }

}
