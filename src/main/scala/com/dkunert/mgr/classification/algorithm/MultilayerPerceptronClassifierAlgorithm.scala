package com.dkunert.mgr.classification.algorithm

import com.dkunert.mgr.ClassificationSparkApplication
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object MultilayerPerceptronClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {

    // specify layers for the neural network:
    // Features, intermediate, intermediate, classes
    val layers = Array[Int](ClassificationSparkApplication.NUMBER_OF_FEATURES , 32, 12, 2)
    //val layers = Array[Int](7, 20, 15, 2)

    return new MultilayerPerceptronClassifier()
      .setLayers(layers)
  }

}
