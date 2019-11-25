#!/bin/sh

echo "Add batch data"
sudo hdfs dfs -put /home/f1db_csv/batch_data/status.csv New_batch_data
sudo hdfs dfs -put /home/f1db_csv/batch_data/races.csv New_batch_data
sudo hdfs dfs -put /home/f1db_csv/batch_data/circuits.csv New_batch_data
sudo hdfs dfs -put /home/f1db_csv/batch_data/lapTimes.csv New_batch_data
sudo hdfs dfs -put /home/f1db_csv/batch_data/results.csv New_batch_data
sudo hdfs dfs -put /home/f1db_csv/batch_data/driver.csv New_batch_data
sudo hdfs dfs -put /home/f1db_csv/batch_data/constructor.csv New_batch_data

echo "Add incremental data"
sudo hdfs dfs -put /home/f1db_csv/incremental_data/results202019.json Incremental_data/Results/results202019.csv
sudo hdfs dfs -put /home/f1db_csv/incremental_data/lapTimes202019.json Incremental_data/Lap_times/lapTimes202019.csv

echo "Add data source"
sudo hdfs dfs -put /home/f1db_csv/data_source/status.csv Data_source/status.csv
sudo hdfs dfs -put /home/f1db_csv/data_source/races.csv Data_source/races.csv
sudo hdfs dfs -put /home/f1db_csv/data_source/circuits.csv Data_source/circuits.csv
sudo hdfs dfs -put /home/f1db_csv/data_source/lapTimes.csv Data_source/lapTimes.csv
sudo hdfs dfs -put /home/f1db_csv/data_source/results.csv Data_source/results.csv
sudo hdfs dfs -put /home/f1db_csv/data_source/driver.csv Data_source/driver.csv
sudo hdfs dfs -put /home/f1db_csv/data_source/constructor.csv Data_source/constructor.csv

echo "Add final data"
sudo hdfs dfs -put /home/f1db_csv/final_data/driver_race.csv Final_data/driver_race.csv
sudo hdfs dfs -put /home/f1db_csv/final_data/race_season.csv Final_data/race_season.csv

echo "Add data source"
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1200_correct_add_batch_data.txt Logs/25112019_1200_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1201_correct_add_batch_data.txt Logs/25112019_1201_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1202_correct_add_batch_data.txt Logs/25112019_1202_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1203_correct_add_batch_data.txt Logs/25112019_1203_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1204_correct_add_batch_data.txt Logs/25112019_1204_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1205_correct_add_batch_data.txt Logs/25112019_1205_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1206_correct_add_batch_data.txt Logs/25112019_1206_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1207_correct_add_batch_data.txt Logs/25112019_1207_correct_add_batch_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1208_correct_add_incremental_data.txt Logs/25112019_1208_correct_add_incremental_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/25112019_1209_correct_add_incremental_data.txt Logs/25112019_1209_correct_add_incremental_data.txt
sudo hdfs dfs -put /home/f1db_csv/logs/register.txt Logs/register.txt 
