package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.clustering.KMeans

object KMeansAlgorithm extends ClusteringAlgorithm {

  def get(): PipelineStage = {
    return new KMeans()
      .setK(2);
  }
}
