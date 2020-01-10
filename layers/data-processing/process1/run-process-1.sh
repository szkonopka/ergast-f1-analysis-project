#!/bin/sh

# Usuwamy timestamp i laczymy pliki ktore zostaly rozdzielone przez flume
echo "Step 1: data preparation"
hdfs dfs -text New_batch_data/lap_times* | hdfs dfs -put - New_batch_data/lap_times.csv
hdfs dfs -rm New_batch_data/lap_times.csv.*
hdfs dfs -text New_batch_data/circuits* | hdfs dfs -put - New_batch_data/circuits.csv
hdfs dfs -rm New_batch_data/circuits.csv.*
hdfs dfs -text New_batch_data/constructors* | hdfs dfs -put - New_batch_data/constructors.csv
hdfs dfs -rm New_batch_data/constructors.csv.*
hdfs dfs -text New_batch_data/races* | hdfs dfs -put - New_batch_data/races.csv
hdfs dfs -rm New_batch_data/races.csv.*
hdfs dfs -text New_batch_data/results* | hdfs dfs -put - New_batch_data/results.csv
hdfs dfs -rm New_batch_data/results.csv.*
hdfs dfs -text New_batch_data/status* | hdfs dfs -put - New_batch_data/status.csv
hdfs dfs -rm New_batch_data/status.csv.*
hdfs dfs -text New_batch_data/driver* | hdfs dfs -put - New_batch_data/driver.csv
hdfs dfs -rm New_batch_data/driver.csv.*

echo "Step 2: processing status.csv"
spark-submit --class Transformation --master local transform.jar status status.csv status 

echo "Step 3: processing drivers.csv"
spark-submit --class Transformation --master local transform.jar driver driver.csv drivers 

echo "Step 4: processing circuits.csv"
spark-submit --class Transformation --master local transform.jar circuit circuits.csv circuits 

echo "Step 5: processing constructors.csv"
spark-submit --class Transformation --master local transform.jar constructor constructors.csv constructors 

echo "Step 6: processing lap_times.csv"
spark-submit --class Transformation --master local transform.jar lapTime lap_times.csv races.csv driver.csv lap_times 

echo "Step 7: processing races.csv"
spark-submit --class Transformation --master local transform.jar race races.csv circuits.csv races

echo "Step 8: processing results.csv"
spark-submit --class Transformation --master local transform.jar result results.csv races.csv driver.csv constructors.csv results 

# Ustrukturyzowanie wynikow przetwarzania 1 
echo "Step 9: structuring the results"
hdfs dfs -text Data_source/status/part* | hdfs dfs -put - Data_source/status.csv
hdfs dfs -rm -r Data_source/status
hdfs dfs -text Data_source/drivers/part* | hdfs dfs -put - Data_source/driver.csv
hdfs dfs -rm -r Data_source/drivers
hdfs dfs -text Data_source/circuits/part* | hdfs dfs -put - Data_source/circuits.csv
hdfs dfs -rm -r Data_source/circuits
hdfs dfs -text Data_source/constructors/part* | hdfs dfs -put - Data_source/constructors.csv
hdfs dfs -rm -r Data_source/constructors
hdfs dfs -text Data_source/lap_times/part* | hdfs dfs -put - Data_source/lap_times.csv
hdfs dfs -rm -r Data_source/lap_times
hdfs dfs -text Data_source/races/part* | hdfs dfs -put - Data_source/races.csv
hdfs dfs -rm -r Data_source/races
hdfs dfs -text Data_source/results/part* | hdfs dfs -put - Data_source/results.csv
hdfs dfs -rm -r Data_source/results

