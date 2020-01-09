import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ProcessingTarget extends Enumeration {
  type ProcessingTarget = Value
  val Laps, Results, Undefined = Value
}

object TranformationJson {
  val NEW_INCREMENTAL_DATA_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/New_incremental_data/"
  val DATA_SOURCE_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/Data_source/status/"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("transformationJson")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    args(1).toLowerCase match {
      case "results" => resultsTransformation("results", sqlContext)
      case "laps" => lapsTransformation("laps", sqlContext)
      case _ => println("Invalid parameter")
    }
  }

  def resultsTransformation(target: String, sqlContext: SQLContext): Unit = {
    val jsonContent = sqlContext.read.json(NEW_INCREMENTAL_DATA_DIR + target)
    println(jsonContent)
  }

  def lapsTransformation(target: String, sqlContext: SQLContext): Unit = {
    val jsonContent = sqlContext.read.json(NEW_INCREMENTAL_DATA_DIR + target)
    println(jsonContent)
  }
}
