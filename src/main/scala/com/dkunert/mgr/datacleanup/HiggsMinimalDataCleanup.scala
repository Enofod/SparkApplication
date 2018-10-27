package com.dkunert.mgr.datacleanup

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object HiggsMinimalDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    var dataset = inputData.select(
      inputData("_c0").cast("double").as("label"),
      inputData("_c22").cast("double"),
      inputData("_c23").cast("double"),
      inputData("_c24").cast("double"),
      inputData("_c25").cast("double"),
      inputData("_c26").cast("double"),
      inputData("_c27").cast("double"),
      inputData("_c28").cast("double")
    )

    dataset = dataset.na.drop()

    val requiredFeatures = Array("_c22", "_c23", "_c24", "_c25", "_c26", "_c27", "_c28")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol(PipelineFactory.FEATURES_KEY)

    return assembler.transform(dataset)
  }
}
