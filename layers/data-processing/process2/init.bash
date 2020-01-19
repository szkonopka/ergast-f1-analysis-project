touch laps.csv
touch results.csv
touch races.csv
touch circuits.csv
touch constructors.csv
touch lap_times.csv
touch status.csv
touch driver.csv
hdfs dfs -put laps.csv Data_source
hdfs dfs -put results.csv Data_source
hdfs dfs -put races.csv Data_source
hdfs dfs -put circuits.csv Data_source
hdfs dfs -put constructors.csv Data_source
hdfs dfs -put lap_times.csv Data_source
hdfs dfs -put status.csv Data_source
hdfs dfs -put driver.csv Data_source
rm laps.csv
rm results.csv
rm races.csv
rm circuits.csv
rm constructors.csv
rm lap_times.csv
rm status.csv
rm driver.csv
