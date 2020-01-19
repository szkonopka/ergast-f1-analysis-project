#!/bin/sh

# lapTimes_20_2019.json for example
lapTimes=$1

# results_20_2019.json for example
results=$2

if [ "$1" = "" ] || [ "$2" = "" ]; then
    echo "No parameters provided"
    exit
fi

cd /home/cloudera/ergast-f1-analysis-project/layers 

# Data processing

hdfs dfs -put ./f1db_csv/incremental_data/$lapTimes New_incremental_data/Lap_times
hdfs dfs -put ./f1db_csv/incremental_data/$results New_incremental_data/Results

sudo bash process2/init.sh

python3.6 process2/transformation.py $lapTimes
python3.6 process2/transformation.py $results