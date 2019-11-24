#!/bin/sh

echo "Current HDFS dir structure"
hdfs dfs -ls

hdfs dfs -mkdir Final_data Data_source New_batch_data New_incremental_data New_incremental_data/Results New_incremental_data/Lap_times Logs

echo "HDFS dir structure after structure creation"
hdfs dfs -ls
hdfs dfs -ls New_incremental_data
