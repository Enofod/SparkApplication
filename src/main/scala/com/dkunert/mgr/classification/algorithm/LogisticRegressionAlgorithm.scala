package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LogisticRegression

object LogisticRegressionAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    return new LogisticRegression()
  }

}
