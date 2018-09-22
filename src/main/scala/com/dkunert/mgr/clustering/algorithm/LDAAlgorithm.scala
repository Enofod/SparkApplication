package com.dkunert.mgr.clustering.algorithm

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.DataFrame

object LDAAlgorithm {

  def run(transformedData: DataFrame): Unit = {
    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(transformedData)

    val ll = model.logLikelihood(transformedData)
    val lp = model.logPerplexity(transformedData)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(transformedData)
    transformed.show(false)
  }
}
