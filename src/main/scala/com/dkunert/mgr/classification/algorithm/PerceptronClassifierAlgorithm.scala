package com.dkunert.mgr.classification.algorithm

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object PerceptronClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](27, 20, 4, 2)

    // Train a GBT model.
    return new MultilayerPerceptronClassifier()
      .setLabelCol(PipelineFactory.INDEXED_LABEL_KEY)
      .setFeaturesCol(PipelineFactory.INDEXED_FEATURES_KEY)
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
  }

}
