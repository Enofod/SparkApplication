package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.DataFrame

object KMeansAlgorithm {

  def run(transformedData: DataFrame): Unit = {
    val kmeans = new KMeans()
      .setK(2)
      .setSeed(1)

    val model = kmeans.fit(transformedData)

    val predictions = model.transform(transformedData)

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println("Silhouette KMeansAlgorithm: ", silhouette)

    model.clusterCenters.foreach(println)

    def cost = model.computeCost(transformedData)
    println("Cost: " + cost)
  }
}
