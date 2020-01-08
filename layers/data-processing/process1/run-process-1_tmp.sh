#!/bin/sh

# Usuwamy timestamp i laczymy pliki ktore zostaly rozdzielone przez flume
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

echo "Step 1: processing status.csv"

echo "Step 2: processing drivers.csv"
spark-submit --class Transformation --master local driverTransform.jar hdfs://quickstart.cloudera:8020/user/cloudera/New_batch_data/driver.csv hdfs://quickstart.cloudera:8020/user/cloudera/Data_source/drivers 

echo "Step 3: processing circuits.csv"

echo "Step 4: processing constructors.csv"

echo "Step 5: processing lap_times.csv"

echo "Step 6: processing races.csv"

echo "Step 7: processing results.csv"

# tutaj mozna jeszcze przetworzyc komendami wyniki przetwarzania na postac pojedynczych csv 
hdfs dfs -text Data_source/drivers/part* | hdfs dfs -put - Data_source/driver.csv
hdfs dfs -rm -r Data_source/drivers

