#!/bin/sh

# Data storage

cd /home/cloudera/ergast-f1-analysis-project/layers 
hive -f ./data-storage/drop-ergast-results.sql
hdfs dfs -rm -r /user/hive/warehouse/*

bash ./data-storage/create-dirs.sh
hive -f ./data-storage/create-hive-ergast-db.sql 
hive -f ./data-storage/create-hive-ergast-db-tables.sql --database ergast_results
hive -f ./data-processing/process3/00-init.sql --database ergast_results

# Data acquisition part 1 - until flume agent start

cd ./data-acquisition
rm -rf /home/ergast
bash ./preparation_to_work.sh 
bash ./start_flume_agentBD.sh
