package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object MultilayerPerceptronClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](28, 20, 15, 2)
    //val layers = Array[Int](7, 20, 15, 2)

    // Train a GBT model.
    return new MultilayerPerceptronClassifier()
      .setLayers(layers)
  }

}
