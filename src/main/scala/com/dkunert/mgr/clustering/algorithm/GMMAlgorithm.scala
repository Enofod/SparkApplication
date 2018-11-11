package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.clustering.GaussianMixture

object GMMAlgorithm extends ClusteringAlgorithm {

  def get(): PipelineStage = {
    return new GaussianMixture();
  }
}
