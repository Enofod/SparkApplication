package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LinearSVC

object LineSupportVectorMachineAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new LinearSVC()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setRegParam(0.1)
  }

}
