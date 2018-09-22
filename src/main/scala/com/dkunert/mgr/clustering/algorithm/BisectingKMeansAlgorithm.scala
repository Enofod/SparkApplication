package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.DataFrame

object BisectingKMeansAlgorithm {

  def run(transformedData: DataFrame): Unit = {
    // Trains a bisecting k-means model.
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val model = bkm.fit(transformedData)

    // Evaluate clustering.
    val cost = model.computeCost(transformedData)
    println(s"Within Set Sum of Squared Errors = $cost")

    // Shows the result.
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)
  }
}
