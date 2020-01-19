#!/bin/sh

numberOfAttempts=5
avgTime=0
csv_path=./incremental/empty-target-csvs

echo "Preparing files for 2nd processing performance tests"
hdfs dfs -put $csv_path/laps.csv Data_source
hdfs dfs -put $csv_path/results.csv Data_source
hdfs dfs -put $csv_path/races.csv Data_source
hdfs dfs -put $csv_path/circuits.csv Data_source
hdfs dfs -put $csv_path/constructors.csv Data_source
hdfs dfs -put $csv_path/lap_times.csv Data_source
hdfs dfs -put $csv_path/status.csv Data_source
hdfs dfs -put $csv_path/driver.csv Data_source

echo "Both files (results and lapTimes) are goint to be processed"
for attempt in $( seq 1 $numberOfAttempts ) 
do

# measure lapTimes processing 2

hdfs dfs -put ./incremental/lapTimes.json New_incremental_data/Lap_times
start=$(date +%s%N)
python3.6 ../data-processing/process2/transformation.py lapTimes.json
echo "Incremental JSON (2nd processing) - LapTimes: " $((($(date +%s%N) - $start) / 1000000)) >> performanceIncMeasurements.txt
lastTime=$(date +%s%N)

# measure results processing 2

hdfs dfs -put ./incremental/results.json New_incremental_data/Results
start=$(date +%s%N)
python3.6 ../data-processing/process2/transformation.py results.json
echo "Incremental JSON (2nd processing) - Results: " $((($(date +%s%N) - $lastTime) / 1000000)) >> performanceIncMeasurements.txt

done
