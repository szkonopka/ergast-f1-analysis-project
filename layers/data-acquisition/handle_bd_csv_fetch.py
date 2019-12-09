import urllib.request
import json
import sys
import os
import posixpath as path
import pandas as pd
import time

ENTRY_CSV_DIR = '/home/ergast/entry'
TRASH_CSV_DIR = '/home/ergast/trash'
INPUT_FILES_DIR = '/home/ergast/input_files'
CSV_EXT = '.csv'

HDFS_USER = 'cloudera'
HDFS_ENDPOINT = 'http://localhost:50070'
HDFS_LOGS_FILE = 'Logs/batch_data.log'

client = InsecureClient(HDFS_ENDPOINT, user=HDFS_USER)
try:
	print(f'Opening HDFS logfile: {HDFS_LOGS_FILE}')
	# Create logfile if doesn't exist
	with client.write(HDFS_LOGS_FILE) as writer:
                writer.write(str.encode(''))
except:
	pass

def log(message):
    message = f'{time.asctime(time.localtime(time.time()))}: {str(message)}\n'
    with client.write(HDFS_LOGS_FILE, append=True) as writer:
        writer.write(str.encode(message))
    print(message)

EXPECTED_FILES_COL_LENGTH = {
    'circuits': 9,
    'constructors': 5,
    'driver': 5,
    'lap_times': 6,
    'races': 8,
    'results': 18,
    'status': 2
}

def validate_csv(file):
	df = pd.read_csv(path.join(ENTRY_CSV_DIR, file), header=None)
	return len(df.columns) == EXPECTED_FILES_COL_LENGTH[file[:-4]]

def move_file_to_dir(file, source_dir_path, target_dir_path):
	log("Send file {} from {} to {}".format(file, source_dir_path, target_dir_path))
	os.replace(path.join(source_dir_path, file), path.join(target_dir_path, file))

log("Step 1 - load all files")
files = (file for file in os.listdir(ENTRY_CSV_DIR) if path.isfile(path.join(ENTRY_CSV_DIR, file)))
csv_files = []

log("Step 2 - fetch only csv files, move non-csv files to trash")
for file in files:
    if file[-4:] != CSV_EXT:
        log("File {} is non-csv file, move it to trash".format(file))
        move_file_to_dir(file, ENTRY_CSV_DIR, TRASH_CSV_DIR)
    else:
        log("File {} is csv file, it will be validate".format(file))
        csv_files.append(file)

log("Step 3 - validate all fetched csv files")
for file in csv_files:
    if (validate_csv(file)):
        log("File {} has proper column size".format(file))
        move_file_to_dir(file, ENTRY_CSV_DIR, INPUT_FILES_DIR)
    else:
        log("File {} has not proper column size, move it to {}".format(file))
        move_file_to_dir(file, ENTRY_CSV_DIR, TRASH_CSV_DIR)
