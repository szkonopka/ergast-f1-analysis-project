import org.apache.spark.SparkContext._ 
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{FileWriter, PrintWriter}

object Transformation {  
	val NEW_BATCH_DATA_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/New_batch_data/"
	val DATA_SOURCE_DIR = "hdfs://quickstart.cloudera:8020/user/cloudera/Data_source/"
	val fileWithResultsPerformance = "/home/performanceScalaTransformation.txt"
	
	def time_standarization (value: String) : Double = {
	   if(value == "\\N" || value == "\\n")
		return 0.toDouble
	   else 
		return value.toInt/1000.toDouble
	}
	def removeQuotes(value: String): String ={
	    return value.replaceAll("\"", "")
	}
	def fastestTime_standarization (value:String):Double  = {
	  val newValue = removeQuotes(value)
	  if(newValue == "\\N" || newValue == "\\n")
		return 0.toDouble
	  
	  var time = newValue
	  if(newValue.startsWith("+"))
		time = value.substring(1)
	  
	  val firstSplit = time.split(":")
	  if(firstSplit.length > 1){
		val minutes = firstSplit(0)
		val secondSplit = firstSplit(1).split("\\.")
		val seconds = secondSplit(0)
		var miliseconds = 0
		if(secondSplit.length > 1){
		  miliseconds += secondSplit(1).toInt
		}	
		return minutes.toInt * 60 + seconds.toInt + miliseconds/1000.toDouble
	  }else{
		val secondSplit = firstSplit(0).split("\\.")
		val seconds = secondSplit(0)
		var miliseconds = 0
		if(secondSplit.length > 1) miliseconds += secondSplit(1).toInt
		return seconds.toInt + miliseconds/1000.toDouble
	  }
	}
	def speed_standarization (value:String):Double  = {
	  val newValue = removeQuotes(value)
	  if(newValue == "\\N" || newValue == "\\n")
		return 0.toDouble
	  return newValue.toDouble
	}
	
	def main(args: Array[String]) {    
		val conf = new SparkConf().setAppName("Transformation")    
		val sc = new SparkContext(conf)

		val typeTransformation = args(0)
		
		val startTime = System.nanoTime()
		if (args(0) == "result") {
		  resultsTransformation(sc, args)
		} else if (args(0) == "driver") {
		  driversTransformation(sc, args)
		} else if (args(0) == "race") {
		  racesTransformation(sc, args)
		} else if (args(0) == "status") {
		  statusTransformation(sc, args)
		} else if (args(0) == "lapTime") {
		  lapTimesTransformation(sc, args)
		} else if (args(0) == "constructor") {
		  constructorsTransformation(sc, args)
		} else if (args(0) == "circuit") {
		  circuitsTransformation(sc, args)
		} else {
		  println("Invalid type parameter")
		}
		val endTime = System.nanoTime()
		val durationMiliseconds = (endTime - startTime) / 1000000
		
		val textToAppend = args(0) + ": " + durationMiliseconds.toString
		FileWriter fileWriter = new FileWriter(fileWithResultsPerformance, true);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		printWriter.println(textToAppend);
		printWriter.close();
	} 
	
	def resultsTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 6)
		val races = sc.textFile(NEW_BATCH_DATA_DIR + args(2))      
				.map(_.split(","))    
				.map(rec => (rec(0).toInt, (rec(2).toInt + rec(1).toInt * 100).toString)) 

		val drivers = sc.textFile(NEW_BATCH_DATA_DIR + args(3))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1)))  

		val constructors = sc.textFile(NEW_BATCH_DATA_DIR + args(4))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1))) 

		val manipulated_races = races.keyBy(t => t._1)
		val manipulated_drivers = drivers.keyBy(t => t._1)
		val manipulated_constructors = constructors.keyBy(t => t._1)

		val results = sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
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
			.map(t => t._2._1._1 + "," + t._2._1._2 + "," + t._2._1._5 + "," + t._2._1._6 + "," + t._2._1._7 + "," + t._2._1._8 + "," + fastestTime_standarization(t._2._1._9.toString) + "," + speed_standarization(t._2._1._10) + "," + t._2._1._11 + "," + time_standarization(t._2._1._12.toString) + "," + t._2._1._3 + "," + t._2._2._2)
			.saveAsTextFile(DATA_SOURCE_DIR + args(5))
	}
	
	def circuitsTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 3)
		sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
			.map(_.split(","))    
			.map(rec => rec(1) + "," + rec(2))     
			.saveAsTextFile(DATA_SOURCE_DIR + args(2))
	}
	
	def constructorsTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 3)
		sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
			.map(_.split(","))    
			.map(rec => rec(1) + "," + rec(2) + "," + rec(3))     
			.saveAsTextFile(DATA_SOURCE_DIR + args(2))
	}
	
	def driversTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 3)
		sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
			.map(_.split(","))    
			.map(rec => rec(1) + "," + rec(4) + "," + rec(5) + "," + rec(7))     
			.saveAsTextFile(DATA_SOURCE_DIR + args(2))
	}
	
	def statusTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 3)
		sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
			.map(_.split(","))    
			.map(rec => if (rec(1).contains("Lap")) rec(0) + "," + "Finished" else rec(0) + "," + rec(1))     
			.saveAsTextFile(DATA_SOURCE_DIR + args(2))
	}
	
	def racesTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 4)
		val circuits = 	sc.textFile(NEW_BATCH_DATA_DIR + args(2))      
					.map(_.split(","))    
					.map(rec => (rec(0).toInt, rec(1))) 

		val races = sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
				.map(_.split(","))    
				.map(rec => ((rec(2).toInt + rec(1).toInt * 100).toString, rec(1), rec(3).toInt)) 

		val manipulated_circuits = circuits.keyBy(t => t._1)
		val manipulated_races = races.keyBy(t => t._3)

		manipulated_races
			.join(manipulated_circuits)
			.map(t => t._2._1._1 + "," + t._2._1._2 + "," + t._2._2._2)
			.saveAsTextFile(DATA_SOURCE_DIR + args(3))
	}
	
	def lapTimesTransformation(sc: SparkContext, args: Array[String]): Unit = {
		checkArguments(args, 5)
		val races = sc.textFile(NEW_BATCH_DATA_DIR + args(2))      
				.map(_.split(","))    
				.map(rec => (rec(0).toInt, (rec(2).toInt + rec(1).toInt * 100).toString)) 

		val drivers = sc.textFile(NEW_BATCH_DATA_DIR + args(3))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1)))  

		val manipulated_races = races.keyBy(t => t._1)
		val manipulated_drivers = drivers.keyBy(t => t._1)

		val lapTimes = sc.textFile(NEW_BATCH_DATA_DIR + args(1))      
			.map(_.split(","))    
			.map(rec => (rec(0).toInt, rec(1).toInt, rec(2), rec(3))) 

		val manipulated_lapTimes = lapTimes.keyBy(t => t._1)

		manipulated_lapTimes
			.join(manipulated_races)
			.map(t => (t._2._2._2, t._2._1._2.toInt, t._2._1._3, t._2._1._4))
			.keyBy(t => t._2)
			.join(manipulated_drivers)
			.map(t => t._2._1._1 + "," + t._2._1._3 + "," + t._2._2._2 + "," + t._2._1._4)
			.saveAsTextFile(DATA_SOURCE_DIR + args(4))
	}
	
	def checkArguments(args: Array[String], expectedNumberArguments: Int){
		if(args.length != expectedNumberArguments){
			println("Invalid number of arguments")
			System.exit(1)
		}
	}
}
