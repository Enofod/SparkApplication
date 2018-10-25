package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.DataFrame

object GMMAlgorithm {

  def run(transformedData: DataFrame): Unit = {
    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
    val model = gmm.fit(transformedData)

    val predictions = model.transform(transformedData)

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println("Silhouette GMMAlgorithm: ", silhouette)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }

  }
}
