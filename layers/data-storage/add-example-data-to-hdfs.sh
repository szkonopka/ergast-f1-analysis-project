#!/bin/sh

echo "Add batch data"
hdfs dfs -put /home/f1db_csv/batch_data/status.csv New_batch_data
hdfs dfs -put /home/f1db_csv/batch_data/races.csv New_batch_data
hdfs dfs -put /home/f1db_csv/batch_data/circuits.csv New_batch_data
hdfs dfs -put /home/f1db_csv/batch_data/lap_times.csv New_batch_data
hdfs dfs -put /home/f1db_csv/batch_data/results.csv New_batch_data
hdfs dfs -put /home/f1db_csv/batch_data/driver.csv New_batch_data
hdfs dfs -put /home/f1db_csv/batch_data/constructors.csv New_batch_data

echo "Add incremental data"
hdfs dfs -put /home/f1db_csv/incremental_data/results202019.json New_incremental_data/Results
hdfs dfs -put /home/f1db_csv/incremental_data/lapTimes202019.json New_incremental_data/Lap_times

echo "Add data source"
hdfs dfs -put /home/f1db_csv/data_source/status.csv Data_source
hdfs dfs -put /home/f1db_csv/data_source/races.csv Data_source
hdfs dfs -put /home/f1db_csv/data_source/circuits.csv Data_source
hdfs dfs -put /home/f1db_csv/data_source/lap_times.csv Data_source
hdfs dfs -put /home/f1db_csv/data_source/results.csv Data_source
hdfs dfs -put /home/f1db_csv/data_source/driver.csv Data_source
hdfs dfs -put /home/f1db_csv/data_source/constructors.csv Data_source

echo "Add final data"
hdfs dfs -put /home/f1db_csv/final_data/driver_race.csv Final_data
hdfs dfs -put /home/f1db_csv/final_data/race_season.csv Final_data

echo "Add data source"
hdfs dfs -put /home/f1db_csv/logs/25112019_1200_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1201_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1202_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1203_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1204_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1205_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1206_correct_add_batch_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1207_correct_add_incremental_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/25112019_1208_correct_add_incremental_data.txt Logs
hdfs dfs -put /home/f1db_csv/logs/register.txt Logs
