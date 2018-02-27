package scala

import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SurvivalPrediction {

  def createDataframe(spark: SparkSession) = {

    val df = spark.read
      .option("header", "true") //reading the headers
      .csv(getClass.getClassLoader.getResource("inp_file1.csv").getPath)

    val someCastedDF = (df.columns.toBuffer).foldLeft(df)((current, c) =>current.withColumn(c, col(c).cast("int")))
    someCastedDF.createOrReplaceTempView("DATA")
    val filteredDF = spark.sql("select * from DATA where Age >0 and PT>=0 and PTT>=0 and Platelets>=0")
    println(filteredDF.count())

    val labelIndexer = new StringIndexer().setInputCol("DOA").setOutputCol("label")
    val labelIndexerModel = labelIndexer.fit(filteredDF)
    val labelDf = labelIndexerModel.transform(filteredDF)

    val assembler = new VectorAssembler().setInputCols(Array("Age", "Gender", "PT", "PTT", "Platelets"))
      .setOutputCol("features")
    val df2 = assembler.transform(labelDf)

    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)

    val classifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(8)
      .setNumTrees(20)
      .setMaxBins(100)
      .setFeatureSubsetStrategy("auto")
      .setSeed(5043)



    val model = classifier.fit(trainingData)
    //println("Random Forest classification model: " + model_rf.toDebugString)
    println("model.featureImportances: " + model.featureImportances)

    val predictions = model.transform(testData)
    val converter = new IndexToString().setInputCol("prediction")
      .setOutputCol("originalValue")
      .setLabels(labelIndexerModel.labels)
    val df3 = converter.transform(predictions)

    df3.select("Age", "Gender", "PT", "PTT",
      "Platelets", "DOA", "label", "prediction", "originalValue").show(5)

    val predictionAndLabels = predictions.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println("RF Test set accuracy = " + evaluator.evaluate(predictionAndLabels))

    GradientBoostingClassifier(df2)

  }

  def GradientBoostingClassifier(dataFrame: DataFrame)={

    val splitSeed = 5043
    val Array(trainingData, testData) = dataFrame.randomSplit(Array(0.7, 0.3), splitSeed)

    val gbtClassifier = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setSeed(5043)
      .setMaxDepth(8)

    val model = gbtClassifier.fit(dataset = trainingData)
    //println("GBT classification model: " + model.toDebugString)
    println("model.featureImportances: " + model.featureImportances)

    val result = model.transform(testData)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println("GBT Test set accuracy = " + evaluator.evaluate(predictionAndLabels))
  }
}