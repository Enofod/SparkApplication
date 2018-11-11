package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.PipelineStage

trait ClusteringAlgorithm {
  def get(): PipelineStage
}

