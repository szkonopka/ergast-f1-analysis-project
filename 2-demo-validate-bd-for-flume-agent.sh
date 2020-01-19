#!/bin/sh

cd /home/cloudera/ergast-f1-analysis-project/layers 

# Data acquisition part 2 - validate and prepare data 

sudo cp -r ./f1db_csv/batch_data/* /home/ergast/entry
sudo python3.6 ./data-acquisition/handle_bd_csv_fetch.py