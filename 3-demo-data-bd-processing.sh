#!/bin/sh

cd /home/cloudera/ergast-f1-analysis-project/layers 

# Data processing

cd ./data-processing/process1
bash ./run-process-1.sh
cd ../ 

hive -f load-csv-files-to-hive.sql --database ergast_results
hive -f process3/01-race_season.sql --database ergast_results
hive -f process3/02-driver_race.sql --database ergast_results