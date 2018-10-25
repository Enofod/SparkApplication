package com.dkunert.mgr.datacleanup

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object PokerDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    inputData.printSchema()
    inputData.head(5).foreach(println);

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
      inputData("_c10").cast("float")
    )

    dataset = dataset.na.drop()

    dataset = new StringIndexer()
      .setInputCol("_c10")
      .setOutputCol("label")
      .setHandleInvalid("keep")
      .fit(dataset).transform(dataset)

    dataset = dataset.drop("_c10")

    val requiredFeatures = Array("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol("features")

    return assembler.transform(dataset)
  }
}
