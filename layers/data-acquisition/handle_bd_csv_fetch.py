import urllib.request
import json
import sys
import os
import prepare_logger
import posixpath as path
import re

def validate_csv(file):
	pass
	
def move_csv_to_input_files(file):
	pass

logger = prepare_logger.get_configured('ergast_handle_api', 'ergast_handle_api.log')

ENTRY_CSV_DIR = '/home/ergast/entry'
TRASH_CSV_DIR = '/home/ergast/trash'
CSV_EXT = '.csv'

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
for file in files:
	if (validate_csv(file)):
		move_csv_to_input_files(file)
