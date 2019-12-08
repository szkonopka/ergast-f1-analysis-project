import urllib.request
import json
import sys
import os
import prepare_logger
import posixpath as path
import pandas as pd

logger = prepare_logger.get_configured('ergast_handle_api', 'ergast_handle_api.log')

ENTRY_CSV_DIR = '/home/ergast/entry'
TRASH_CSV_DIR = '/home/ergast/trash'
INPUT_FILES_DIR = '/home/ergast/input_files'
CSV_EXT = '.csv'

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
	
def move_csv_to_input_files(file):
	logger.debug("Send file {} to {}".format(file, INPUT_FILES_DIR))
	os.replace(path.join(ENTRY_CSV_DIR, file), path.join(INPUT_FILES_DIR, file))

logger.debug("Step 1 - load all files")
files = (file for file in os.listdir(ENTRY_CSV_DIR) if path.isfile(path.join(ENTRY_CSV_DIR, file)))
csv_files = []

logger.debug("Step 2 - fetch only csv files, move non-csv files to trash")
for file in files:
	if file[-4:] != CSV_EXT:
		logger.debug("File {} is non-csv file, move it to trash".format(file))
		os.replace(path.join(ENTRY_CSV_DIR, file), path.join(TRASH_CSV_DIR, file))
	else:
		logger.debug("File {} is csv file, it will be validate".format(file))
		csv_files.append(file)

logger.debug("Step 3 - validate all fetched csv files")
for file in csv_files:
	if (validate_csv(file)):
		logger.debug("File {} has proper column size".format(file))
		move_csv_to_input_files(file)