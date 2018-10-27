package com.dkunert.mgr.datacleanup

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object HiggsDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    //inputData.printSchema()
    //inputData.head(5).foreach(println);

    var dataset = inputData.select(
      inputData("_c0").cast("float"),
      inputData("_c1").cast("float"),
      inputData("_c2").cast("float"),
      inputData("_c3").cast("float"),
      inputData("_c4").cast("float"),
      inputData("_c5").cast("float"),
      inputData("_c6").cast("float"),
      inputData("_c7").cast("float"),
      inputData("_c8").cast("float"),
      inputData("_c9").cast("float"),
      inputData("_c10").cast("float"),
      inputData("_c11").cast("float"),
      inputData("_c12").cast("float"),
      inputData("_c13").cast("float"),
      inputData("_c14").cast("float"),
      inputData("_c15").cast("float"),
      inputData("_c16").cast("float"),
      inputData("_c17").cast("float"),
      inputData("_c18").cast("float"),
      inputData("_c19").cast("float"),
      inputData("_c20").cast("float"),
      inputData("_c21").cast("float"),
      inputData("_c22").cast("float"),
      inputData("_c23").cast("float"),
      inputData("_c24").cast("float"),
      inputData("_c25").cast("float"),
      inputData("_c26").cast("float"),
      inputData("_c27").cast("float")
    )

    dataset = dataset.na.drop()

    dataset = new StringIndexer()
      .setInputCol("_c0")
      .setOutputCol(PipelineFactory.LABEL_KEY)
      .setHandleInvalid("keep")
      .fit(dataset).transform(dataset)

    dataset = dataset.drop("_c0")

    val requiredFeatures = Array("_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13",
      "_c14", "_c15", "_c16", "_c17", "_c18", "_c19", "_c20", "_c21", "_c22", "_c23", "_c24", "_c25", "_c26", "_c27")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol(PipelineFactory.FEATURES_KEY)

    return assembler.transform(dataset)
  }
}
