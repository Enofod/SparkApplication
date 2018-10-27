package com.dkunert.mgr.datacleanup

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object HiggsMinimalDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    var dataset = inputData.select(
      inputData("_c0").cast("double").as("label"),
      inputData("_c1").cast("double"),
      inputData("_c2").cast("double"),
      inputData("_c3").cast("double"),
      inputData("_c4").cast("double"),
      inputData("_c5").cast("double"),
      inputData("_c6").cast("double"),
      inputData("_c7").cast("double"),
      inputData("_c8").cast("double"),
      inputData("_c9").cast("double"),
      inputData("_c10").cast("double"),
      inputData("_c11").cast("double"),
      inputData("_c12").cast("double"),
      inputData("_c13").cast("double"),
      inputData("_c14").cast("double"),
      inputData("_c15").cast("double"),
      inputData("_c16").cast("double"),
      inputData("_c17").cast("double"),
      inputData("_c18").cast("double"),
      inputData("_c19").cast("double"),
      inputData("_c20").cast("double"),
      inputData("_c21").cast("double"),
      inputData("_c22").cast("double"),
      inputData("_c23").cast("double"),
      inputData("_c24").cast("double"),
      inputData("_c25").cast("double"),
      inputData("_c26").cast("double"),
      inputData("_c27").cast("double"),
      inputData("_c28").cast("double")
    )

    dataset = dataset.na.drop()

    val requiredFeatures = Array("_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13",
      "_c14", "_c15", "_c16", "_c17", "_c18", "_c19", "_c20", "_c21", "_c22", "_c23", "_c24", "_c25", "_c26", "_c27")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol(PipelineFactory.FEATURES_KEY)

    return assembler.transform(dataset)
  }
}
