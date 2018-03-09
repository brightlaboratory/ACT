/* SimpleApp.scala */


import breeze.linalg.*
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col


object SimpleApp {
  def createDataframe(spark: SparkSession) = {

    val df = spark.read
      .option("header", "false") //reading the headers
      .csv(getClass.getClassLoader.getResource("train").getPath)

    //df.printSchema()

    val someCastedDF = (df.columns.toBuffer).foldLeft(df)((current, c)
    => current.withColumn(c, col(c).cast("double")))
    someCastedDF.createOrReplaceTempView("DATA")

    val newNamesTrain = Seq("id", "Age", "Gender", "PT", "PTT", "Platelets", "DOA")
    val renamedDF = someCastedDF.toDF(newNamesTrain: _*)

    println(renamedDF.count())
    //renamedDF.printSchema()

    val labelIndexer = new StringIndexer().setInputCol("DOA").setOutputCol("label")
    val labelIndexerModelTrain = labelIndexer.fit(renamedDF)
    val labelDfTrain = labelIndexerModelTrain.transform(renamedDF)

    val assemblerTrain = new VectorAssembler().setInputCols(Array( "Age", "Gender", "PT", "PTT", "Platelets"))
      .setOutputCol("features")
    val dfTrain = assemblerTrain.transform(labelDfTrain)

    //Reading test dataset
    val dfTest = spark.read
      .option("header", "false") //reading the headers
      .csv(getClass.getClassLoader.getResource("test").getPath)

    //df.printSchema()

    val someCastedTestDF = (dfTest.columns.toBuffer).foldLeft(dfTest)((current, c)
    => current.withColumn(c, col(c).cast("double")))
    someCastedDF.createOrReplaceTempView("DATA")

    val newNamesTest = Seq("id", "Age", "Gender", "PT", "PTT", "Platelets", "DOA")
    val renamedTestDF = someCastedTestDF.toDF(newNamesTest: _*)

    println(renamedTestDF.count())
    //renamedTestDF.printSchema()

    //val labelIndexer = new StringIndexer().setInputCol("DOA").setOutputCol("label")
    val labelIndexerModelTest = labelIndexer.fit(renamedTestDF)
    val labelDfTest = labelIndexerModelTest.transform(renamedTestDF)

    val assemblerTest = new VectorAssembler().setInputCols(Array( "Age", "Gender", "PT", "PTT", "Platelets"))
      .setOutputCol("features")
    val dfTest_ = assemblerTest.transform(labelDfTest)

    //RFC(dfTrain, dfTest_, labelIndexer)
 // }

  //def RFC(train: DataFrame, test: DataFrame, labelIndexer: StringIndexer): Unit ={

    //val labelIndexerModel = labelIndexer.fit(test)

    val classifier = new RandomForestClassifier()
    .setImpurity("gini")
    .setMaxDepth(4)
    .setNumTrees(50)
    .setMaxBins(100)
    .setFeatureSubsetStrategy("auto")
    .setSeed(5043)

    val model = classifier.fit(dfTrain)

    println("model.featureImportances: " + model.featureImportances)

    val predictions = model.transform(dfTest_)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("originalValue")
      .setLabels(labelIndexerModelTest.labels)

    val dfPred = labelConverter.transform(predictions)
    dfPred.createOrReplaceTempView("DATA")

    val tp = spark.sql("select count(*) from DATA where label = 1.0 and prediction = 1.0")
    val tn = spark.sql("select count(*) from DATA where label = 0.0 and prediction = 0.0")
    val fp = spark.sql("select count(*) from DATA where label = 0.0 and prediction = 1.0")
    val fn = spark.sql("select count(*) from DATA where label = 1.0 and prediction = 0.0")
    val count = spark.sql("select count(*) from DATA")

    val accuracy = (tp.first().getLong(0) + tn.first().getLong(0))/count.first().getLong(0)
    val precision = tp.first().getLong(0)/(tp.first().getLong(0)+ fp.first().getLong(0))

    //Column/Row seq: "True Positive" , "False Positive", "False Negative", "True Negative"
    val confMatDF = tp.union(fp).union(fn).union(tn)
    println(accuracy +"    "+ precision)
    println(confMatDF.show())
    //println("True Positive: "+tp.first().getLong(0))
    //print("True Negative: "+tn.first().getLong(0))
    //println("False Positive: "+fp.first().getLong(0))
    //print("False Negative: "+fn.first().getLong(0))
  }

}