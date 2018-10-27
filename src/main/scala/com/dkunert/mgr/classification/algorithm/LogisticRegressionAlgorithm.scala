package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LogisticRegression

object LogisticRegressionAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {

    // Trains a bisecting k-means model.
    return new LogisticRegression()
      //.setMaxIter(10)
      //.setRegParam(0.3)
      //.setElasticNetParam(0.8)
      //.setFamily("multinomial")
  }

}
