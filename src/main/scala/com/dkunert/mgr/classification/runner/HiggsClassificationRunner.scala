package com.dkunert.mgr.classification.runner

import com.dkunert.mgr.classification.algorithm.ClassificationAlgorithm
import com.dkunert.mgr.classification.pipeline.PipelineFactory
import com.dkunert.mgr.util.BenchmarkUtil
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrame

object HiggsClassificationRunner {

  def run(algorithm: ClassificationAlgorithm, cleanedData: DataFrame, trainingData: DataFrame, testData: DataFrame, iteration: Double): List[Double] = {
    val pipeline = PipelineFactory.getPipeline(algorithm.get(), cleanedData)

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

    val accuracyEvaluator = new BinaryClassificationEvaluator().setLabelCol(PipelineFactory.LABEL_KEY)
    val areaUnderROCEvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC").setLabelCol(PipelineFactory.LABEL_KEY)
    val areaUnderPREvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderPR").setLabelCol(PipelineFactory.LABEL_KEY)
    val accuracy = accuracyEvaluator.evaluate(predictions)

    System.out.println("Accuracy = " + accuracy)
    val areaUnderPR = areaUnderPREvaluator.evaluate(predictions)
    System.out.println("areaUnderPR = " + areaUnderPR)

    System.gc()
    //val accuracy = evaluator.evaluate(predictions)
    //println(className + " Precyzja = " + accuracy)
    println("------------------------------------------------------------------------------------------------------------")

    return List(iteration, trainTime, testTime, accuracy, accuracy, areaUnderPR)
  }

}
