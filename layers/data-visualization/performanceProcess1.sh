#!/bin/sh

numberAttempts=5
averageTime=0
for i in $( seq 1 $numberAttempts )
do
start=$(date +%s%N)
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar status status.csv status 
echo "Status: " $((($(date +%s%N) - $start)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar driver driver.csv drivers 
echo "Drivers: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar circuit circuits.csv circuits 
echo "Circuits: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar constructor constructors.csv constructors 
echo "Constructors: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar lapTime lap_times.csv races.csv driver.csv lap_times 
echo "Lap times: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar race races.csv circuits.csv races
echo "Races: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar result results.csv races.csv driver.csv constructors.csv results 
echo "Results: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt

end=$(date +%s%N)
echo -e "Pomiar przetwazania 1 [ms] proba $i : " $((($end - $start)/1000000)) "\n" >> performanceMeasurements.txt
echo -e "Pomiar przetwazania 1 [ms] proba $i : " $((($end - $start)/1000000)) "\n"
echo -e "\n" >> performanceScalaWithSaveToHDFS.txt
echo -e "\n" >> performanceScalaTransformation.txt
averageTime=$(($averageTime+$((($end - $start)/1000000))))
hdfs dfs -rm -r Data_source/status
hdfs dfs -rm -r Data_source/drivers
hdfs dfs -rm -r Data_source/circuits
hdfs dfs -rm -r Data_source/constructors
hdfs dfs -rm -r Data_source/lap_times
hdfs dfs -rm -r Data_source/races
hdfs dfs -rm -r Data_source/results
done

averageTime=$(($averageTime/$numberAttempts))
echo -e "Sredni czas przetwazania 1 [ms]: " $averageTime "\n" >> performanceMeasurements.txt


