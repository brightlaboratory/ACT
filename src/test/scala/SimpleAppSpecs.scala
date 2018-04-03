
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class SimpleAppSpecs extends FlatSpec with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _
  private var spark: SparkSession = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
    spark = SparkSession.builder.
      master(master)
      .appName("spark session example")
      .getOrCreate()
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "This test" should "generate metrics" in {
    GradientBoosting.createDataframe(spark)
  }
}
