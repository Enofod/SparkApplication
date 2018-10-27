package com.dkunert.mgr.classification.algorithm

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.RandomForestClassifier

object RandomForrestClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new RandomForestClassifier()
      .setLabelCol(PipelineFactory.INDEXED_LABEL_KEY)
      .setFeaturesCol(PipelineFactory.INDEXED_FEATURES_KEY)
      .setNumTrees(10)


    //val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    //println("Learned classification forest model:\n" + rfModel.toDebugString)
  }
}
