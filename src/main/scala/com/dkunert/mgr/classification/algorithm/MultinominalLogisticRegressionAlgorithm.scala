package com.dkunert.mgr.classification.algorithm

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

object MultinominalLogisticRegressionAlgorithm {

  def run(transformedData: DataFrame): Unit = {
    // Trains a bisecting k-means model.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(transformedData)

    // Print the coefficients and intercept for multinomial logistic regression
    println(s"Coefficients: \n${lrModel.coefficientMatrix}")
    println(s"Intercepts: ${lrModel.interceptVector}")

    var predictions = lrModel.transform(transformedData);

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
  }

}
