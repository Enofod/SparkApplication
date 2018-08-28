package com.dkunert.mgr

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object SparkApplication {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Titatnic app")
      .config("spark.master", "local")
      .getOrCreate();

    val rawData = spark.read
      .format("csv")
      .option("header", "true")
      .load("F:\\bigdata\\titanic\\all\\train.csv")

    rawData.printSchema()
    rawData.head(5).foreach(println);

    var dataset = rawData.select(
      rawData("Survived").cast("float"),
      rawData("Pclass").cast("float"),
      rawData("Sex"),
      rawData("Age").cast("float"),
      rawData("Fare").cast("float"),
      rawData("Embarked")
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

    val transformedData = assembler.transform(dataset)

    transformedData.head(5).foreach(println)

    val kmeans = new KMeans()
      .setK(5)
      .setSeed(1)

    val model = kmeans.fit(transformedData)

    val clusteredData = model.transform(transformedData)

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(clusteredData)

    println("Silhouette: ", silhouette)

    model.clusterCenters.foreach(println)
  }
}
