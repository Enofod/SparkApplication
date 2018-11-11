package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.clustering.BisectingKMeans

object BisectingKMeansAlgorithm extends ClusteringAlgorithm {

  def get(): PipelineStage = {
    return new BisectingKMeans();
  }
}
