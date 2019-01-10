package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier

object DecisionTreeClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new DecisionTreeClassifier()
  }

}
