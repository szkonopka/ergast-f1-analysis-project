import org.apache.spark.SparkContext._ 
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Transformation {  
	def main(args: Array[String]) {    
		val conf = new SparkConf().setAppName("Transformation")    
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		val df = sqlContext.read.option("multiline",true).json(args(0))
		//val df = sqlContext.read.json(args(0))

		df.registerTempTable("dummyTable")		

		df.select("MRData.RaceTable.round").show()
		
		val round = df.select("MRData.RaceTable.round").collectAsList().get(0);
		val season = df.select("MRData.RaceTable.season").collectAsList().get(0);
		println("Round: " + round)
		println("Season: " + season)

		val races = df.select("MRData.RaceTable.Races").collectAsList()
		val firstRace = races.get(0)
println(firstRace)
		df.printSchema()
		
		//val race = sqlContext.sql("select MRData.RaceTable.Races[0] from dummyTable")
		 
		//println(race)

		//println(race.select("Circuit.circuitId"))

		//sqlContext.sql("select MRData.RaceTable.Races[0] from dummyTable").printSchema()

		//df.show()
	} 
}
