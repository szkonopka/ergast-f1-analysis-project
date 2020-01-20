#!/bin/sh


hdfs dfs -rm New_batch_data/*
hdfs dfs -rm -r Data_source/*
hdfs dfs -cp Datasets/20seasonFlume/* New_batch_data/
echo -e "\n 20 seasons \n" >> performanceMeasurements.txt
echo -e "\n 20 seasons \n" >> performanceScalaTransformation.txt
echo -e "\n 20 seasons \n" >> performanceScalaWithSaveToHDFS.txt

numberAttempts=1
averageTime=0
for i in $( seq 1 $numberAttempts )
do
start=$(date +%s%N)
hdfs dfs -text New_batch_data/status* | hdfs dfs -put - New_batch_data/status.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar status status.csv status 
hdfs dfs -text Data_source/status/part* | hdfs dfs -put - Data_source/status.csv
echo "Status: " $((($(date +%s%N) - $start)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

hdfs dfs -text New_batch_data/driver* | hdfs dfs -put - New_batch_data/driver.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar driver driver.csv drivers 
hdfs dfs -text Data_source/drivers/part* | hdfs dfs -put - Data_source/driver.csv
echo "Drivers: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

hdfs dfs -text New_batch_data/circuits* | hdfs dfs -put - New_batch_data/circuits.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar circuit circuits.csv circuits 
hdfs dfs -text Data_source/circuits/part* | hdfs dfs -put - Data_source/circuits.csv
echo "Circuits: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

hdfs dfs -text New_batch_data/constructors* | hdfs dfs -put - New_batch_data/constructors.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar constructor constructors.csv constructors 
hdfs dfs -text Data_source/constructors/part* | hdfs dfs -put - Data_source/constructors.csv
echo "Constructors: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

hdfs dfs -text New_batch_data/races* | hdfs dfs -put - New_batch_data/races.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar race races.csv circuits.csv races
hdfs dfs -text Data_source/races/part* | hdfs dfs -put - Data_source/races.csv
echo "Races: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

hdfs dfs -text New_batch_data/lap_times* | hdfs dfs -put - New_batch_data/lap_times.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar lapTime lap_times.csv races.csv driver.csv lap_times 
hdfs dfs -text Data_source/lap_times/part* | hdfs dfs -put - Data_source/lap_times.csv
echo "Lap times: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt
lastTime=$(date +%s%N) 

hdfs dfs -text New_batch_data/results* | hdfs dfs -put - New_batch_data/results.csv
spark-submit --class Transformation --master local transformWithPerformanceMeasure.jar result results.csv races.csv driver.csv constructors.csv results 
hdfs dfs -text Data_source/results/part* | hdfs dfs -put - Data_source/results.csv
echo "Results: " $((($(date +%s%N) - $lastTime)/1000000)) >> performanceMeasurements.txt

end=$(date +%s%N)
echo -e "Pomiar przetwazania 1 [ms] proba $i : " $((($end - $start)/1000000)) "\n" >> performanceMeasurements.txt
echo -e "Pomiar przetwazania 1 [ms] proba $i : " $((($end - $start)/1000000)) "\n"
echo -e "\n" >> performanceScalaWithSaveToHDFS.txt
echo -e "\n" >> performanceScalaTransformation.txt
averageTime=$(($averageTime+$((($end - $start)/1000000))))
hdfs dfs -rm New_batch_data/*.csv
hdfs dfs -rm Data_source/*.csv
hdfs dfs -rm -r Data_source/*
#hdfs dfs -cp -rf Datasets/AllSeasonsFlume/* New_batch_data/
done

averageTime=$(($averageTime/$numberAttempts))
echo -e "Sredni czas przetwazania 1 [ms]: " $averageTime "\n" >> performanceMeasurements.txt
