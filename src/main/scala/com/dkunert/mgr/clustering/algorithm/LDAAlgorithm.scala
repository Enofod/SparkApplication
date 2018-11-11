package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.clustering.LDA

object LDAAlgorithm extends ClusteringAlgorithm {

  def get(): PipelineStage = {
    return new LDA();
  }
}
