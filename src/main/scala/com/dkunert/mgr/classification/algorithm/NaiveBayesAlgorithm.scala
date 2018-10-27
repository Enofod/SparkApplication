package com.dkunert.mgr.classification.algorithm

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.NaiveBayes

object NaiveBayesAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {

    return new NaiveBayes()
      .setLabelCol(PipelineFactory.INDEXED_LABEL_KEY)
      .setFeaturesCol(PipelineFactory.INDEXED_FEATURES_KEY)
  }

}
