import urllib.request
import json
import sys
import os
import time
import json
import re
from hdfs import InsecureClient

HDFS_USER = 'cloudera'
HDFS_ENDPOINT = 'http://localhost:50070'
HDFS_NEW_INCREMENTAL_DATA = 'New_incremental_data'
HDFS_DATA_SOURCE = 'Data_source'

class Lap:
	def __init__(driverId, position, time)
		self.driverId = driverId
		self.position = position
		self.time = time

	def get_json_map():
		data = {}
		data['driverId'] = self.driverId
		data['position'] = self.position
		data['time'] = self.time
		return data

# Logging
client = InsecureClient(HDFS_ENDPOINT, user=HDFS_USER)
try:
	print(f'Opening HDFS logfile: {HDFS_LOGS_FILE}')
	# Create logfile if doesn't exist
	with client.write(HDFS_LOGS_FILE) as writer:
                writer.write(str.encode(''))
except:
	pass

def log(message):
	message = f'{time.asctime(time.localtime(time.time()))}: {message}\n'
	with client.write(HDFS_LOGS_FILE, append=True) as writer:
		writer.write(str.encode(message))
	print(message)

def get_json(path):
	with client.read(path) as json_file:
		data = json.load(json_file)
		return data

def save_as_csv(path, data):
	pass
	with client.write(path) as write:
		writer.write(data)
		
def process_results(filename):
	data = get_json(NEW_INCREMENTAL_DATA + "/Results/" + filename)
	for node in data['Results']
		pass
	pass

def process_laps(filename):
	laps = []
	data = get_json(NEW_INCREMENTAL_DATA + "/Laps/" + filename)
	for node in data['Laps']
		lap = Lap(node['driverId'], node['position'], node['time'])
		laps.append(lap)
	pass
	# prepare raw string with csv format

filename = sys.argv[1]

results_pattern = r'results\_.*\_.*\.json*'
lapTimes_pattern = r'lapTimes\_.*\_.*\.json*'

if (re.match(results_pattern, filename)):
	process_results()
else if (re.match(lap_times_pattern, filename)):
	process_laps()
else:
	print("Invalid argument")
