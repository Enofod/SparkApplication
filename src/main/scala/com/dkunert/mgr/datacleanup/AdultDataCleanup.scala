package com.dkunert.mgr.datacleanup

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object AdultDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    inputData.printSchema()
    inputData.head(5).foreach(println);

    var dataset = inputData.select(
      inputData("_c0").cast("float"),
      inputData("_c1"),
      inputData("_c2").cast("float"),
      inputData("_c3"),
      inputData("_c4").cast("float"),
      inputData("_c5"),
      inputData("_c6"),
      inputData("_c7"),
      inputData("_c8"),
      inputData("_c9"),
      inputData("_c10").cast("float"),
      inputData("_c11").cast("float"),
      inputData("_c12").cast("float"),
      inputData("_c13")
    )

    dataset = changeDataset(dataset, 1)
    dataset = changeDataset(dataset, 3)
    dataset = changeDataset(dataset, 5)
    dataset = changeDataset(dataset, 6)
    dataset = changeDataset(dataset, 7)
    dataset = changeDataset(dataset, 8)
    dataset = changeDataset(dataset, 9)
    dataset = changeDataset(dataset, 13)

    dataset = new StringIndexer()
      .setInputCol("_c14")
      .setOutputCol("label")
      .setHandleInvalid("keep")
      .fit(dataset).transform(dataset)

    dataset = dataset.drop("_c14")

    val requiredFeatures = Array("_c0", "d1", "_c2", "d3", "_c4", "d5", "_c6", "d7", "d8", "d9", "_c10", "_c11", "_c12", "_c13")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol("features")

    return assembler.transform(dataset)
  }

  def changeDataset(dataset: DataFrame, number:Int): DataFrame = {
    var indexedDataset = new StringIndexer()
      .setInputCol("_c" + number)
      .setOutputCol("d" + number)
      .setHandleInvalid("keep")
      .fit(dataset).transform(dataset)

    indexedDataset = indexedDataset.na.drop("_c" + number)
    return indexedDataset
  }
}
