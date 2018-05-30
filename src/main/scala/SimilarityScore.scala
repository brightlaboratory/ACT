import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SimilarityScore {


  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def createDataframe(spark: SparkSession) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val path = getClass.getClassLoader.getResource("train").getPath

    println("path: " + path)

    val files = getListOfFiles(path)
    files.foreach(file => println("FILE: " + file))

    if (files.size >= 2) {
      val df1 = spark.read
        .option("header", "false") //reading the headers
        .csv(files.head.toString)

      val df2 = spark.read
        .option("header", "false")
        .csv(files(1).toString)


      val dist = computeAverageDistance(
        addColumnNames(convertStringColumnsToDouble(df1)),
        addColumnNames(convertStringColumnsToDouble(df2)))
      println("dist: " + dist)
    }

  }

  def addColumnNames(df: DataFrame) = {
    val newNamesTrain = Seq("id", "Age", "Gender", "PT", "PTT", "Platelets", "DOA")
    df.toDF(newNamesTrain: _*)
  }

  def convertStringColumnsToDouble(df: DataFrame) = {
    df.columns.toBuffer.foldLeft(df)((current, c)
    => current.withColumn(c, col(c).cast("double")))
  }


  def rowWiseDistUDF = udf((row1: Row, row2: Row) => {
    var sum:Double = 0
    for (i <- 0 to (row1.size - 1)) {
      sum += Math.abs(row1.getDouble(i) - row2.getDouble(i))
    }

    sum / row1.size
  })

  def computeAverageDistance(df1: DataFrame, df2: DataFrame): Double = {

    println("df1.count(): " + df1.count())
    println("df2.count(): " + df2.count())

    val (df1Normalized, df2Normalized) = normalizeAgeColumn(df1, df2)

    val df1Mod = addSuffixToColumnNames(df1Normalized, "df1")
    val df2Mod = addSuffixToColumnNames(df2Normalized, "df2")

    df1Mod.take(1).foreach(row => println("ROW: " + row))
    df2Mod.take(1).foreach(row => println("ROW: " + row))

    val crossJoinDf = df1Mod.crossJoin(df2Mod)
    crossJoinDf.printSchema()
    crossJoinDf.take(1).foreach(row => println("row: " + row))

    val df1Names = crossJoinDf.columns.filter(_.endsWith("df1")).map(col)
    val df2Names = crossJoinDf.columns.filter(_.endsWith("df2")).map(col)

    crossJoinDf.withColumn("distance", rowWiseDistUDF(struct(df1Names: _*),
      struct
      (df2Names: _*)))
      .take(1)
      .foreach(v =>
        println("df1: " + v))

    // Now we have to match rows of df1 with rows of df2

    0.0
  }

  def addSuffixToColumnNames(df: DataFrame, suffix: String) = {
    df.toDF(df.columns.map(str => str + "_" + suffix): _*)
  }

  def normalizeAgeColumn(df1: DataFrame, df2: DataFrame) = {

    val maxAge = Math.max(df1.select(max("Age")).head().getDouble(0),
      df2.select(max("Age")).head().getDouble(0))

    val minAge = Math.min(df1.select(min("Age")).head().getDouble(0),
      df2.select(min("Age")).head().getDouble(0))

    val range = maxAge - minAge

    (normalizeAgeColumnOfDf(df1, minAge, range),
      normalizeAgeColumnOfDf(df2, minAge, range))
  }

  def normalizeAgeUDF = udf((age:Double, minAge: Double, range: Double) => {
    (age - minAge) / range
  })

  def normalizeAgeColumnOfDf(df: DataFrame, minAge: Double, range: Double) = {
    df.withColumn("NewAge", normalizeAgeUDF(df("Age"), lit(minAge), lit(range)))
      .drop("Age")
      .withColumnRenamed("NewAge", "Age")
  }

}
