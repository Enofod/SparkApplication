package com.dkunert.mgr.datacleanup

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object TitanicDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    inputData.printSchema()
    inputData.head(5).foreach(println);

    var dataset = inputData.select(
      inputData("Survived").cast("float"),
      inputData("Pclass").cast("float"),
      inputData("Sex"),
      inputData("Age").cast("float"),
      inputData("Fare").cast("float"),
      inputData("Embarked")
    )

    dataset = dataset.na.drop()

    dataset.head(5).foreach(println)

    dataset = new StringIndexer()
      .setInputCol("Sex")
      .setOutputCol("Gender")
      .setHandleInvalid("keep")
      .fit(dataset).transform(dataset)

    dataset = new StringIndexer()
      .setInputCol("Embarked")
      .setOutputCol("Boarded")
      .setHandleInvalid("keep")
      .fit(dataset).transform(dataset)

    dataset = dataset.drop("Sex")
    dataset = dataset.drop("Embarked")
    dataset.head(5).foreach(println)

    val requiredFeatures = Array("Survived", "Pclass", "Age", "Fare", "Gender", "Boarded")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol("features")

    return assembler.transform(dataset)
  }
}
