package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier

object DecisionTreeClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new DecisionTreeClassifier()

    /*val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)*/
  }

}
