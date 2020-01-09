import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._ 

object ProcessingTarget extends Enumeration {
  type ProcessingTarget = Value
  val Laps, Results, Undefined = Value
}

object TransformationJson {
  val NEW_INCREMENTAL_DATA_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/New_incremental_data/"
  val DATA_SOURCE_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/Data_source/status/"

  def main(args: Array[String]) {
    println("######## JSON TRANSFORMATION ########")
   
    val filename = args(0)
    println("Given file " + filename)

    val conf = new SparkConf().setAppName("transformationJson")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val resultsPattern = """results\_.*\_.*\.json*"""
    val lapTimesPattern = """lapTimes\_.*\_.*\.json*"""

    if (filename.matches(resultsPattern)) {
      resultsTransformation(filename, sqlContext, sc)
    } else if (filename.matches(lapTimesPattern)) {
      lapsTransformation(filename, sqlContext, sc)
    } else {
      println("Invalid parameter")
    }
  }

  def resultsTransformation(target: String, sqlContext: SQLContext, sc: SparkContext): Unit = {
    println("Processing... " + NEW_INCREMENTAL_DATA_DIR + target)
    val path: String = NEW_INCREMENTAL_DATA_DIR + target
    val jsonContent = sqlContext.read.option("multiline",true).json(path)
    println(jsonContent)
  }

  def lapsTransformation(target: String, sqlContext: SQLContext, sc: SparkContext): Unit = {
    println("Processing..." + NEW_INCREMENTAL_DATA_DIR + target)
    val path: String = NEW_INCREMENTAL_DATA_DIR + target
    val jsonContent = sqlContext.read.option("multiline",true).json(path)
    println(jsonContent)
  }
}
