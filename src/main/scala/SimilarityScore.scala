import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window

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
      /*
      println("files.head: " + files.head)
      println("files(1): " + files(1))
      var df1 = spark.read
        .option("header", "false") //reading the headers
        .csv(files.head.toString)

      var df2 = spark.read
        .option("header", "false")
        .csv(files(1).toString)
      */
      var j = 0
      var k = 0
      var count = 0
      val file = new File("ds_distance.csv")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("File1" + "," + "File2"+"," + "avgDistance, minDistance, maxDistance, zeroDistPercent" + "\n")
      while (j < files.size - 1){
        k = j + 1
        while (k < files.size){
          var df1 = spark.read
              .option("header", "false")
              .csv(files(j).toString)

          var df2 = spark.read
              .option("header", "false")
              .csv(files(k).toString)

          println(files(j))
          println(files(k))

          // Need to uncomment and run the below lines within while loop
          var results = computeAverageDistanceEfficient(
            addColumnNames(convertStringColumnsToDouble(df1)),
            addColumnNames(convertStringColumnsToDouble(df2)))

          bw.write(files(j).getName+ ","+ files(k).getName+ ","+results._1+ ","+results._2+ ","+results._3+ ","+results._4 + "\n")
          println("dist: " + results)
          k += 1
          count += 1
        }
        j += 1
      }
      println(count)
      bw.close()


      // TODO: Each configuration has 10 files. We want to call : Done
      // "computeAverageDistance" function on all 10C2 = 45 combinations of
      // files and record the data.
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

//    println("row1.size: " + row1.size)

    // We will exclude id column at index 0
    for (i <- 1 until row1.size) {
      sum += Math.abs(row1.getDouble(i) - row2.getDouble(i))
    }

    sum / (row1.size - 1)
  })

  def rowSumUDF = udf((row1: Row) => {
    var sum: Double = 0

    // We will exclude id column at index 0
    for (i <- 1 until row1.size) {
      sum += row1.getDouble(i)
    }

    sum
  })

  def computeAverageDistance(df1: DataFrame, df2: DataFrame) = {

    import df1.sparkSession.implicits._

    println("df1.count(): " + df1.count())
    println("df2.count(): " + df2.count())

    val (df1Normalized, df2Normalized) = normalizeAgeColumn(df1, df2)

    val df1Mod = addSuffixToColumnNames(df1Normalized, "df1")
    val df2Mod = addSuffixToColumnNames(df2Normalized, "df2")

    df1Mod.take(1).foreach(row => println("ROW: " + row))
    df2Mod.take(1).foreach(row => println("ROW: " + row))

    var distanceSum: Double = 0
    var numRows: Double = 0
    var zeroDistRows: Double = 0
    var maxDistance: Double = 0
    var minDistance: Double = 1 // the distance cannot exceed 1

    val df2ModCopy = df2Mod
    //      .where($"id_df2" < 10)
    val df1ModCopy = df1Mod
    //      .where($"id_df1" < 10)
    val df1Ids = df1ModCopy.select("id_df1").collect().map(row => row
      .getAs[Double]
      ("id_df1")).toList

    println("df1ModCopy.count(): " + df1ModCopy.count())
    println("df2ModCopy.count(): " + df2ModCopy.count())

    var df2Ids = List[Double]()
    df1Ids.foreach(df1Id => {
      val crossJoinDf = df1ModCopy.where($"id_df1" === df1Id)
        .crossJoin(df2ModCopy.where(!($"id_df2".isin(df2Ids: _*))))
      val df1Names = crossJoinDf.columns.filter(_.endsWith("df1")).map(col)
      val df2Names = crossJoinDf.columns.filter(_.endsWith("df2")).map(col)

      val crossJoinDfSimilarity = crossJoinDf.withColumn("distance",
        rowWiseDistUDF(struct(df1Names: _*), struct(df2Names: _*)))

      val minRow = crossJoinDfSimilarity.orderBy(asc("distance")).head()
      //      println("minRow: " + minRow)
      val df2Id = minRow.getAs[Double]("id_df2")
      df2Ids = df2Id :: df2Ids
      val distance = minRow.getAs[Double]("distance")
      //println("df1Id: " + df1Id + " df2Id: " + df2Id + " distance: " + distance)

      distanceSum += distance
      numRows += 1

      if (distance == 0) {
        zeroDistRows += 0
      }

      maxDistance = Math.max(maxDistance, distance)
      minDistance = Math.min(minDistance, distance)
    })

    println("df2ModCopy.count(): " + df2ModCopy.count())
    val avgDistance = distanceSum / numRows
    val zeroDistPercent = (distanceSum / numRows) * 100.0

    println("avgDistance: " + avgDistance)
    println("minDistance: " + minDistance)
    println("maxDistance: " + maxDistance)
    println("zeroDistPercent: " + zeroDistPercent)
    (avgDistance, minDistance, maxDistance, zeroDistPercent)
  }

  def computeAverageDistanceEfficient(df1: DataFrame, df2: DataFrame) = {

    import df1.sparkSession.implicits._

    println("df1.count(): " + df1.count())
    println("df2.count(): " + df2.count())

    val (df1Normalized, df2Normalized) = normalizeAgeColumn(df1, df2)

    val df1Mod = addSuffixToColumnNames(df1Normalized, "df1")
    val df2Mod = addSuffixToColumnNames(df2Normalized, "df2")

    df1Mod.take(1).foreach(row => println("ROW: " + row))
    df2Mod.take(1).foreach(row => println("ROW: " + row))

    var distanceSum: Double = 0
    var numRows: Double = 0
    var zeroDistRows: Double = 0
    var maxDistance: Double = 0
    var minDistance: Double = 1 // the distance cannot exceed 1

    val df2ModCopy = df2Mod
    //      .where($"id_df2" < 10)
    val df2Count = df2Mod.count().toDouble
    val numRowsToConsider = 20 // TODO : Play with this number
    val df1ModCopy = df1Mod
    //      .where($"id_df1" < 10)
    val df1Ids = df1ModCopy.select("id_df1").collect().map(row => row
      .getAs[Double]
      ("id_df1")).toList

    println("df1ModCopy.count(): " + df1ModCopy.count())
    println("df2ModCopy.count(): " + df2ModCopy.count())

    var df2Ids = List[Double]()
    var count:Double = 0
    df1Ids.foreach(df1Id => {
      val crossJoinDf = df1ModCopy.where($"id_df1" === df1Id)
        .crossJoin(df2ModCopy.where(!($"id_df2".isin(df2Ids: _*)))
          .sample(withReplacement = false,
            fraction = (numRowsToConsider / (df2Count - count)),
            seed = 100)
        ).limit(10)
      val df1Names = crossJoinDf.columns.filter(_.endsWith("df1")).map(col)
      val df2Names = crossJoinDf.columns.filter(_.endsWith("df2")).map(col)

      val crossJoinDfSimilarity = crossJoinDf.withColumn("distance",
        rowWiseDistUDF(struct(df1Names: _*), struct(df2Names: _*)))

      val minRow = crossJoinDfSimilarity.orderBy(asc("distance")).head()
      //      println("minRow: " + minRow)
      val df2Id = minRow.getAs[Double]("id_df2")
      df2Ids = df2Id :: df2Ids
      val distance = minRow.getAs[Double]("distance")
//      println("df1Id: " + df1Id + " df2Id: " + df2Id + " distance: " + distance)

      distanceSum += distance
      numRows += 1

      if (distance == 0) {
        zeroDistRows += 0
      }

      maxDistance = Math.max(maxDistance, distance)
      minDistance = Math.min(minDistance, distance)

      count += 1
    })

    println("df2ModCopy.count(): " + df2ModCopy.count())
    val avgDistance = distanceSum / numRows
    val zeroDistPercent = (distanceSum / numRows) * 100.0

    println("avgDistance: " + avgDistance)
    println("minDistance: " + minDistance)
    println("maxDistance: " + maxDistance)
    println("zeroDistPercent: " + zeroDistPercent)
    (avgDistance, minDistance, maxDistance, zeroDistPercent)
  }


  def addSumColumn(df: DataFrame) = {
    val dfNames = df.columns.map(col)
    df.withColumn("sum", rowSumUDF(struct(dfNames: _*)))
  }

  def addRankColumn(df: DataFrame) = {
    import df.sparkSession.implicits._
    val idColumn = df.columns.filter(col => col.startsWith("id")).head
    val w = Window.orderBy($"sum", $"$idColumn")
    val rankColumn = "rank" + idColumn.substring(2)
    df.withColumn(rankColumn, rank().over(w))
  }

  def computeAverageDistanceApproximate(df1: DataFrame, df2: DataFrame) = {

    import df1.sparkSession.implicits._
    println("df1.count(): " + df1.count())
    println("df2.count(): " + df2.count())
    val (df1Normalized, df2Normalized) = normalizeAgeColumn(df1, df2)

    val df1Mod = addSuffixToColumnNames(df1Normalized, "df1")
    val df2Mod = addSuffixToColumnNames(df2Normalized, "df2")

    df1Mod.take(1).foreach(row => println("ROW: " + row))
    df2Mod.take(1).foreach(row => println("ROW: " + row))

    val df1Names = df1Mod.columns.filter(_.endsWith("df1")).map(col)
    val df2Names = df2Mod.columns.filter(_.endsWith("df2")).map(col)

    val df2ModCopy = addRankColumn(addSumColumn(df2Mod))
    //      .where($"id_df2" < 10)
    val df1ModCopy = addRankColumn(addSumColumn(df1Mod))
    //      .where($"id_df1" < 10)


//    df1ModCopy.show(100, truncate = false)
//    df2ModCopy.show(100, truncate = false)

    val joinedDf = df1ModCopy.join(df2ModCopy, $"rank_df1" === $"rank_df2",
      "inner")
      .withColumn("distance", rowWiseDistUDF(struct(df1Names: _*), struct(df2Names: _*)))

    joinedDf.show(100, truncate = false)

    val resultRow = joinedDf.select(avg("distance") as "average",
      max("distance") as "maximum",
      min("distance") as "minimum")
      .head()

    val avgDistance = resultRow.getAs[Double]("average")
    val minDistance = resultRow.getAs[Double]("minimum")
    val maxDistance = resultRow.getAs[Double]("maximum")

    val zeroCount = joinedDf.where($"distance" <= 0.0).count()
    val totalCount = joinedDf.count()
    val zeroDistPercent = (zeroCount.toDouble * 100.0) / totalCount.toDouble

    println("avgDistance: " + avgDistance)
    println("minDistance: " + minDistance)
    println("maxDistance: " + maxDistance)
    println("zeroDistPercent: " + zeroDistPercent)
    (avgDistance, minDistance, maxDistance, zeroDistPercent)
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
