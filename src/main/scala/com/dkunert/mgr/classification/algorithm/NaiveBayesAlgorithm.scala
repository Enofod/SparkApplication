package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.NaiveBayes

object NaiveBayesAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {

    return new NaiveBayes()
  }

}
