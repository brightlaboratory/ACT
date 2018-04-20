
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object GradientBoosting {
  def createDataframe(spark: SparkSession) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val df = spark.read
      .option("header", "false") //reading the headers
      .csv(getClass.getClassLoader.getResource("train").getPath)

    //df.printSchema()

    val someCastedDF = (df.columns.toBuffer).foldLeft(df)((current, c)
    => current.withColumn(c, col(c).cast("double")))
    //someCastedDF.createOrReplaceTempView("DATA")

    val newNamesTrain = Seq("id", "Age", "Gender", "PT", "PTT", "Platelets","HF", "DOA")
    val renamedDF = someCastedDF.toDF(newNamesTrain: _*)


    val k = 10000
    val w = Window.orderBy("id", "Age", "Gender", "PT", "PTT", "Platelets", "HF", "DOA")
    val indexedDF = renamedDF.withColumn("index", row_number().over(w))
    indexedDF.createOrReplaceTempView("INPUT")

    var low = 0
    var high = low + k
    var counter = 0
    val arrayLength = (indexedDF.count() / k).toInt
    var accuracy = new Array[Double](arrayLength)
    var recall = new Array[Double](arrayLength)
    var precision = new Array[Double](arrayLength)
    var f1 = new Array[Double](arrayLength)

    while (counter < arrayLength) {
      //print(low+ ":::"+ high)
      var testDF = spark.sql(s"select index, id, Age, Gender, PT, PTT, Platelets, HF, DOA from INPUT where index <= ${high} and index > ${low}")
      var trainDF = spark.sql(s"select index, id, Age, Gender, PTT, PT, Platelets, HF, DOA from INPUT where index <= ${low} or index > ${high}")
      //println(testDF.first())
      //println(trainDF.first())
      low = low + k.toInt

      high = high + k
      var labelIndexerTrain = new StringIndexer().setInputCol("DOA").setOutputCol("label")
      var labelIndexerModelTrain = labelIndexerTrain.fit(trainDF)
      var labelDfTrain = labelIndexerModelTrain.transform(trainDF)

      var assembler = new VectorAssembler().setInputCols(Array("Age", "Gender", "PT", "PTT", "Platelets", "HF"))
        .setOutputCol("features")
      var transformedDf = assembler.transform(labelDfTrain)

      var labelIndexerTest = new StringIndexer().setInputCol("DOA").setOutputCol("label")
      var labelIndexerModelTest = labelIndexerTest.fit(testDF)
      var labelDfTest = labelIndexerModelTest.transform(trainDF)

      var DfTest = assembler.transform(labelDfTest)

      var classifier = new GBTClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(100)
        .setSeed(5043)
        .setMaxDepth(8)

      var model = classifier.fit(transformedDf)

      println("model.featureImportances: " + model.featureImportances)

      var predictions = model.transform(DfTest)

      var labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("originalValue")
        .setLabels(labelIndexerModelTest.labels)

      var dfPred = labelConverter.transform(predictions)
      dfPred.createOrReplaceTempView("DATA")

      var tp = spark.sql("select count(*) from DATA where label = 1.0 and prediction = 1.0")
      var tn = spark.sql("select count(*) from DATA where label = 0.0 and prediction = 0.0")
      var fp = spark.sql("select count(*) from DATA where label = 0.0 and prediction = 1.0")
      var fn = spark.sql("select count(*) from DATA where label = 1.0 and prediction = 0.0")
      var count = spark.sql("select count(*) from DATA")

      accuracy(counter) = (tp.first().getLong(0) + tn.first().getLong(0)).toFloat / count.first().getLong(0).toFloat
      precision(counter) = tp.first().getLong(0).toFloat / (tp.first().getLong(0) + fp.first().getLong(0)).toFloat
      recall(counter) = tp.first().getLong(0).toFloat / (tp.first().getLong(0) + fn.first().getLong(0)).toFloat
      f1(counter) = 2.0 / ((1 / recall(counter)) + (1 / precision(counter)))
      counter = counter + 1
    }
    val metricsName = "num,accuracy,precision,recall,f1\n"
    var i = 0
    val file = new File("Metrics_GBTC.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(metricsName)
    println(metricsName)
    while (i < arrayLength) {
      var str = i.toString() + "," + accuracy(i).toString() + "," + precision(i).toString() + "," + recall(i).toString() + "," + f1(i).toString() + "\n"
      bw.write(str)
      println(str)
      i = i + 1
    }
    bw.close()
  }
}
