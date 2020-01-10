# Data storage

cd /home/cloudera/ergast-f1-analysis-project/layers 
hdfs dfs -rm -r /user/hive/warehouse/*
# DROP DATABASE ergast_results CASCADE;

bash data-storage/create-dirs.sh
hive -f data-storage/create-hive-ergast-db.sql 
hive -f data-storage/create-hive-ergast-db-tables.sql --database ergast_results
hive -f data-processing/process3/00-init.sql --database	ergast_results

# Data acquisition

cd data-acquisition/
rm -rf /home/ergast
bash preparation_to_work.sh 
bash start_flume_agentBD.sh
cp ../f1db_csv/batch_data_test/* /home/ergast/input_files/

hdfs dfs -put ../f1db_csv/incremental_data/lapTimes_20_2019.json New_incremental_data/Lap_times
hdfs dfs -put ../f1db_csv/incremental_data/results_20_2019.json New_incremental_data/Results

# Data processing

cd ../data-processing/process1/
bash run-process-1.sh
cd ../ 

hive -f load-csv-files-to-hive.sql --database ergast_results

hive -f process3/01-race_season.sql --database ergast_results
hive -f process3/02-driver_race.sql --database ergast_results

python3.6 process2/transformation.py lapTimes_20_2019.json
python3.6 process2/transformation.py results_20_2019.json

