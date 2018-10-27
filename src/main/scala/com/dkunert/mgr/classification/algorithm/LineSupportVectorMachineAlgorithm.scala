package com.dkunert.mgr.classification.algorithm

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LinearSVC

object LineSupportVectorMachineAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new LinearSVC()
      .setLabelCol(PipelineFactory.INDEXED_LABEL_KEY)
      .setFeaturesCol(PipelineFactory.INDEXED_FEATURES_KEY)
      .setMaxIter(10)
      .setRegParam(0.1)
  }

}
