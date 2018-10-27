package com.dkunert.mgr.classification.algorithm

import com.dkunert.mgr.classification.pipeline.PipelineFactory
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier

object DecisionTreeClassifierAlgorithm extends ClassificationAlgorithm {

  def get(): PipelineStage = {

        // Split the data into training and test sets (30% held out for testing).
    //val Array(trainingData, testData) = transformedData.randomSplit(Array(0.7, 0.3))

    return new DecisionTreeClassifier()
      .setLabelCol(PipelineFactory.INDEXED_LABEL_KEY)
      .setFeaturesCol(PipelineFactory.INDEXED_FEATURES_KEY)

    /*// Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy = " + accuracy)

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)*/
  }

}
