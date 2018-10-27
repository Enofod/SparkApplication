package com.dkunert.mgr.classification.pipeline

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

object PipelineFactory {

  val FEATURES_KEY = "features"
  val LABEL_KEY = "label"
  val PREDICTION_KEY = "prediction"

  def getPipeline(algorithm: PipelineStage, data: DataFrame): Pipeline = {

    val pipeline = new Pipeline()
      .setStages(Array(algorithm))

    return pipeline
  }

}
