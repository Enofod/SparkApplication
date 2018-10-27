package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.RandomForestClassifier

object RandomForrestClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new RandomForestClassifier()

    //val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    //println("Learned classification forest model:\n" + rfModel.toDebugString)
  }
}
