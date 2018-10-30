package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.GBTClassifier

object GradientBoostedTreeClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new GBTClassifier()

    /* val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")*/
  }

}
