package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.GBTClassifier

object GradientBoostedTreeClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {

    // Train a GBT model.
    return new GBTClassifier()
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

   /* // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy = " + accuracy)

    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")*/
  }

}
