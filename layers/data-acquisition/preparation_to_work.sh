#!/bin/sh

# hdfs structure must be created before use acquisition layer srcipts 

# create spooling directory
mkdir /home/ergast
mkdir /home/ergast/input_files

cp flumeAgentBDConfig.properties /home/
cp flumeAgentIDConfig.properties /home/
cp start_flume_agentBD.sh /home/
cp start_flume_agentID.sh /home/

chmod +x /home/start_flume_agentBD.sh
chmod +x /home/start_flume_agentID.sh

# starts scripts must run in different terminals 

# sample tests of the operation of the acquisition layer after run flume agents 
# cp -a ../f1db_csv/data_source/status.csv /home/ergast/input_files/
# cp -a ../f1db_csv/incremental_data/results_18_2019.json /home/ergast/input_files/

