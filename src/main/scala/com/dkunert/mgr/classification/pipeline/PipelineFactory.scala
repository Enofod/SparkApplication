package com.dkunert.mgr.classification.pipeline

import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

object PipelineFactory {

  val FEATURES_KEY = "features"
  val INDEXED_FEATURES_KEY = "indexedFeatures"

  val LABEL_KEY = "label"
  val INDEXED_LABEL_KEY = "indexedLabel"

  val PREDICTION_KEY = "prediction"
  val PREDICTION_LABEL_KEY = "predictedLabel"

  def getPipeline(algorithm: PipelineStage, data: DataFrame): Pipeline = {

    val labelIndexer = new StringIndexer()
      .setInputCol(LABEL_KEY)
      .setOutputCol(INDEXED_LABEL_KEY)
      .fit(data)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol(FEATURES_KEY)
      .setOutputCol(INDEXED_FEATURES_KEY)
      .setMaxCategories(9) // features with > 4 distinct values are treated as continuous.
      .fit(data)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol(PREDICTION_KEY)
      .setOutputCol(PREDICTION_LABEL_KEY)
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, algorithm, labelConverter))

    return pipeline
  }

}
