import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import java.io.{BufferedWriter, File, FileWriter}

object FeatureStats {
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

    //val newNamesTrain = Seq("id", "Age", "Gender", "PT", "PTT", "Platelets", "Hyperfibrinolysis", "DOA")
    val newNamesTrain = Seq("id", "Age", "Gender", "PT", "PTT", "Platelets", "DOA")
    val renamedDF = someCastedDF.toDF(newNamesTrain: _*)


    //val w = Window.orderBy("id", "Age", "Gender", "PT", "PTT", "Platelets", "Hyperfibrinolysis", "DOA")
    val w = Window.orderBy("id", "Age", "Gender", "PT", "PTT", "Platelets", "DOA")
    val indexedDF = renamedDF.withColumn("index", row_number().over(w))
    indexedDF.createOrReplaceTempView("INPUT")

    val k = 10000
    var low = 0
    var high = low + k
    var counter = 0
    val arrayLength = (indexedDF.count()/k).toInt
    val file = new File("Coeffs.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    while(counter<arrayLength){
      var testDF = spark.sql(s"select index, id, Age, Gender, PT, PTT, Platelets, DOA from INPUT where index <= ${high} and index > ${low}")
      low = low + k.toInt
      high = high + k
      //println(low, high, testDF.count())
      var assembler = new VectorAssembler().setInputCols(Array( "Age", "Gender", "PT", "PTT", "Platelets", "DOA"))
        .setOutputCol("features")
      var transformedDf = assembler.transform(testDF)

      val Row(coeff1: Matrix) = Correlation.corr(transformedDf, "features").head

      val Row(coeff2: Matrix) = Correlation.corr(transformedDf, "features", "spearman").head

      writeToFile(coeff1,bw, 1)
      writeToFile(coeff2,bw, 2)
      counter += 1
    }
    bw.close()
  }

  def writeToFile(coeff: Matrix, bufferedWriter: BufferedWriter, flag: Int): Unit ={


    var localMatrix: List[Array[Double]] = coeff
      .transpose  // Transpose since .toArray is column major
      .toArray
      .grouped(coeff.numCols)
      .toList

    var lines: List[String] = localMatrix
      .map(line => line.mkString(" "))
    val colNames = Array("Age", "Gender", "PT", "PTT", "Platelets", "DOA")
    if (flag == 1){
      bufferedWriter.write("Pearson correlation matrix. \n\n")
    }
    else{
      bufferedWriter.write("Spearman correlation matrix. \n\n")
    }
    bufferedWriter.write(" Age Gender PT PTT Platelets DOA\n")
    var c = 0
    for (l <- lines) {
      //println(colNames(c)+ "  "+l)
      bufferedWriter.write(colNames(c)+ " "+l +"\n")
      c += 1
    }

    //bw.write("Age Gender PT PTT Platelets DOA \n")
    //bw.write(coeff1.toString())
    bufferedWriter.write("\n\n")
  }
}
