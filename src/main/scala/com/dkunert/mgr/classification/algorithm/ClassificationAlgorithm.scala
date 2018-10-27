package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage

trait ClassificationAlgorithm {
  def get(): PipelineStage
}

