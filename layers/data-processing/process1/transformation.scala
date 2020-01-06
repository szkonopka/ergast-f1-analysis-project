import org.apache.spark.SparkContext._ 
import org.apache.spark.{SparkConf, SparkContext}

object Test {  
	def bigDecimal_standarization (value: String) : BigDecimal ={
	   if(value == "\\N" || value == "\\n")
		return BigDecimal(0)
	   else 
		return BigDecimal(value)
	}
	def time_standarization (value: String) : Double = {
	   if(value == "\\N" || value == "\\n")
		return 0.toDouble
	   else 
		return value.toInt/1000.toDouble
	}
	def fastestTime_standarization (value: String) : String = {
	   if(value == "\\N" || value == "\\n")
		return "0"
	   else 
		return value
	}
	def main(args: Array[String]) {    
		val conf = new SparkConf().setAppName("Test")    
		val sc = new SparkContext(conf)


		val races = sc.textFile(args(1))      
				.map(_.split(";"))    
				.map(rec => (rec(0).toInt, (rec(2).toInt + rec(1).toInt * 100).toString)) 

		val drivers = sc.textFile(args(2))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1)))  

		val constructors = sc.textFile(args(3))      
			.map(_.split(";"))    
			.map(rec => (rec(0).toInt, rec(1))) 

		val manipulated_races = races.keyBy(t => t._1)
		val manipulated_drivers = drivers.keyBy(t => t._1)
		val manipulated_constructors = constructors.keyBy(t => t._1)

		val results = sc.textFile(args(0))      
			.map(_.split(","))    
			.filter(rec => (rec(5).toInt > 0 && rec(10).toInt > 0))
			.map(rec => (rec(0), rec(1).toInt, rec(2).toInt, rec(3).toInt, rec(5), rec(8), rec(17), rec(9), rec(15), rec(16), rec(10), rec(12))) 

		val manipulated_results = results.keyBy(t => t._2)

		manipulated_results
			.join(manipulated_races)
			.map(t => (t._2._1._1, t._2._2._2, t._2._1._3.toInt, t._2._1._4.toInt, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._1._9, t._2._1._10, t._2._1._11, t._2._1._12))
			.keyBy(t => t._3)
			.join(manipulated_drivers)
			.map(t => (t._2._1._1, t._2._1._2, t._2._2._2, t._2._1._4.toInt, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._1._9, t._2._1._10, t._2._1._11, t._2._1._12))
			.keyBy(t => t._4)
			.join(manipulated_constructors)
			.map(t => t._2._1._1 + ";" + t._2._1._2 + ";" + t._2._1._5 + ";" + t._2._1._6 + ";" + t._2._1._7 + ";" + t._2._1._8 + ";" + fastestTime_standarization(t._2._1._9.toString) + ";" + t._2._1._10 + ";" + t._2._1._11 + ";" + time_standarization(t._2._1._12.toString) + ";" + t._2._1._3 + ";" + t._2._2._2)
			.saveAsTextFile(args(4))

		/* Lap times
		val races = sc.textFile(args(1))      
				.map(_.split(";"))    
				.map(rec => (rec(0).toInt, (rec(2).toInt + rec(1).toInt * 100).toString)) 

		val drivers = sc.textFile(args(2))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1)))  

		val manipulated_races = races.keyBy(t => t._1)
		val manipulated_drivers = drivers.keyBy(t => t._1)

		val lapTimes = sc.textFile(args(0))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1).toInt, rec(2), rec(3))) 

		val manipulated_lapTimes = lapTimes.keyBy(t => t._1)

		manipulated_lapTimes
			.join(manipulated_races)
			.map(t => (t._2._2._2, t._2._1._2.toInt, t._2._1._3, t._2._1._4))
			.keyBy(t => t._2)
			.join(manipulated_drivers)
			.map(t => t._2._1._1 + ";" + t._2._1._3 + ";" + t._2._2._2 + ";" + t._2._1._4)
			.saveAsTextFile(args(3))*/

		/* Races
		val circuits = 	sc.textFile(args(1))      
					.map(_.split(","))    
					.map(rec => (rec(0).toInt, rec(1))) 

		val races = sc.textFile(args(0))      
				.map(_.split(";"))    
				.map(rec => ((rec(2).toInt + rec(1).toInt * 100).toString, rec(1), rec(3).toInt)) 

		val manipulated_circuits = circuits.keyBy(t => t._1)
		val manipulated_races = races.keyBy(t => t._3)

		manipulated_races
			.join(manipulated_circuits)
			.map(t => t._2._1._1 + ";" + t._2._1._2 + ";" + t._2._2._2)
			.saveAsTextFile(args(2))*/

		/* Status
		sc.textFile(args(0))      
			.map(_.split(","))    
			.map(rec => if (rec(1).contains("Laps")) rec(0) + ";" + "Finished" else rec(0) + ";" + rec(1))     
			.saveAsTextFile(args(1))*/

		/* Circuits
		sc.textFile(args(0))      
			.map(_.split(","))    
			.map(rec => rec(1) + ";" + rec(2))     
			.saveAsTextFile(args(1))*/

		/* Drivers
		sc.textFile(args(0))      
			.map(_.split(","))    
			.map(rec => rec(1) + ";" + rec(4) + ";" + rec(5) + ";" + rec(7))     
			.saveAsTextFile(args(1))*/

		/* Constructors
		sc.textFile(args(0))      
			.map(_.split(";"))    
			.map(rec => rec(1) + ";" + rec(2) + ";" + rec(3))     
			.saveAsTextFile(args(1))*/

		/*sc.textFile(args(0))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1).toInt))      
			.reduceByKey((a, b) => Math.max(a, b))      
			.saveAsTextFile(args(1))  */
	} 
}
