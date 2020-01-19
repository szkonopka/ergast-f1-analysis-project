#!/bin/bash

DATASET=$1  # Dataset dir name in layers/data-visualization
ATTEMPTS=2  # Number of repeated measurements
PROCESS_1_TIME=0
PROCESS_3_TIME=0

cd /home/cloudera/ergast-f1-analysis-project/layers
echo "RESULTS WILL BE WRITTEN TO FILE data-visualization/perf-test/output/${DATASET}.txt"
echo -e "Performance test results\n" > data-visualization/perf-test/output/${DATASET}.txt

for i in $( seq 1 $ATTEMPTS )
do
  
  echo 'CLEARING ENVIRONMENT...'

  {
  cd /home/cloudera/ergast-f1-analysis-project/layers 
  hdfs dfs -rm -r /user/hive/warehouse/*
  hive -e "DROP DATABASE ergast_results CASCADE";
  } &> /dev/null

  echo 'PREPARING ENVIRONMENT...'
  {
  bash data-storage/create-dirs.sh
  hive -f data-storage/create-hive-ergast-db.sql 
  hive -f data-storage/create-hive-ergast-db-tables.sql --database ergast_results
  hive -f data-processing/process3/00-init.sql --database ergast_results
  hdfs dfs -put data-visualization/$DATASET/* New_batch_data
  } &> /dev/null

  echo "RUNNING PROCESS 1..."

  PROCESS_1_START_TIME=$(date +%s%N)
  
  cd data-processing/process1
  bash run-process-1.sh &> /dev/null
  cd ../../

  PROCESS_1_END_TIME=$(date +%s%N)
  PROCESS_1_TIME=$(($PROCESS_1_TIME+$((($PROCESS_1_END_TIME - $PROCESS_1_START_TIME)/1000000))))
  echo -e "Process 1, attempt ${i}: " $((($PROCESS_1_END_TIME - $PROCESS_1_START_TIME)/1000000)) " [ms] \n" >> data-visualization/perf-test/output/${DATASET}.txt

  echo "RUNNING PROCESS 2..."

  PROCESS_2_START_TIME=$(date +%s%N)
  
  echo "Loading csv files to hive..."
  hive -f data-processing/load-csv-files-to-hive.sql --database ergast_results &> /dev/null
  echo "Processing race_season..."
  hive -f data-processing/process3/01-race_season.sql --database ergast_results &> /dev/null
  echo "Processing driver_race..."
  hive -f data-processing/process3/02-driver_race.sql --database ergast_results &> /dev/null

  PROCESS_2_END_TIME=$(date +%s%N)
  PROCESS_2_TIME=$(($PROCESS_2_TIME+$((($PROCESS_2_END_TIME - $PROCESS_2_START_TIME)/1000000))))
  echo -e "Process 2, attempt ${i}: " $((($PROCESS_2_END_TIME - $PROCESS_2_START_TIME)/1000000)) " [ms] \n" >> data-visualization/perf-test/output/${DATASET}.txt
  
done

PROCESS_1_AVG=$(($PROCESS_1_TIME/$ATTEMPTS))
PROCESS_2_AVG=$(($PROCESS_2_TIME/$ATTEMPTS))

echo -e "\n-----------\nProcess 1 average: ${PROCESS_1_AVG} [ms] \n" >> data-visualization/perf-test/output/${DATASET}.txt
echo -e "Process 2 average: ${PROCESS_2_AVG} [ms] \n" >> data-visualization/perf-test/output/${DATASET}.txt

echo "RESULTS WRITTEN TO FILE: data-visualization/perf-test/output/${DATASET}.txt"
