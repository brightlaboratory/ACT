import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SimilarityScore {


  def getListOfFiles(dir: String): List[File] = {
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
    var sum: Double = 0

    // We will exclude id column at index 0
    for (i <- 1 to (row1.size - 1)) {
      sum += Math.abs(row1.getDouble(i) - row2.getDouble(i))
    }

    sum / (row1.size - 1)
  })

  def computeAverageDistance(df1: DataFrame, df2: DataFrame): Double = {

    import df1.sparkSession.implicits._

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

    val crossJoinDfSimilarity = crossJoinDf.withColumn("distance",
      rowWiseDistUDF(struct(df1Names: _*),
        struct
        (df2Names: _*)))

    var crossJoinDfSimilarityCopy = crossJoinDfSimilarity

    while (!crossJoinDfSimilarityCopy.head(1).isEmpty) {
      val df1Id = crossJoinDfSimilarityCopy.select(min("id_df1") as "id_df1").head()
        .getAs[Double]("id_df1")
      val minRow = crossJoinDfSimilarityCopy.where($"id_df1" === df1Id).orderBy(asc
      ("distance"))
        .head()
      val df2Id = minRow.getAs[Double]("id_df2")
      crossJoinDfSimilarityCopy = crossJoinDfSimilarityCopy.filter(not($"id_df1"
        === df1Id))
      println("df1Id: " + df1Id + " df2Id: " + df2Id + " distance: " + minRow
        .getAs[Double]("distance"))
    }

    //    crossJoinDfSimilarity.show(10, truncate = false)
    //    // Now we have to match rows of df1 with rows of df2
    //    val pairedIds = df1Mod.map(row => {
    //      val df1Id = row.getAs[Double]("id_df1")
    //
    //      println("value:")
    //      crossJoinDfSimilarity.show(10, truncate = false)
    //      val value = crossJoinDfSimilarity.where($"id_df1" === df1Id)
    //      value.show(100, truncate = false)
    //
    //      val df2Id = crossJoinDfSimilarity.where($"id_df1" === df1Id)
    //        .select(min("distance"))
    //        .head().getAs[Double]("id_df2")
    //
    //      crossJoinDfSimilarity
    //        .filter(not($"id_df1" === df1Id and $"id_df2" === df2Id))
    //      (df1Id, df2Id)
    //    })
    //
    //    val pairedIdsDf = pairedIds.toDF("id_df1", "id_df2")
    //    pairedIdsDf.show(10, truncate = false)

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

  def normalizeAgeUDF = udf((age: Double, minAge: Double, range: Double) => {
    (age - minAge) / range
  })

  def normalizeAgeColumnOfDf(df: DataFrame, minAge: Double, range: Double) = {
    df.withColumn("NewAge", normalizeAgeUDF(df("Age"), lit(minAge), lit(range)))
      .drop("Age")
      .withColumnRenamed("NewAge", "Age")
  }

}
