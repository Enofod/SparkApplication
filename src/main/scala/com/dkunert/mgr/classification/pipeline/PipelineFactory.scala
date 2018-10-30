package com.dkunert.mgr.classification.pipeline

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

object PipelineFactory {

  val FEATURES_KEY = "features"
  val UNPROCESSED_FEATURES_KEY = "unprocessedFeatures"
  val LABEL_KEY = "label"
  val PREDICTION_KEY = "prediction"

  def getPipeline(algorithm: PipelineStage, data: DataFrame): Pipeline = {

    val indexer = new VectorIndexer()
      .setInputCol(UNPROCESSED_FEATURES_KEY)
      .setOutputCol(FEATURES_KEY)
      .setMaxCategories(12)
      .fit(data)


    val pipeline = new Pipeline()
      .setStages(Array(indexer, algorithm))

    return pipeline
  }

}
