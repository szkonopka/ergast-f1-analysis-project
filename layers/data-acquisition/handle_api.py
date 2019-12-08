import urllib.request
import json
import sys
import os
import time
from hdfs import InsecureClient


# Constants
#ERGAST_ENDPOINT = 'http://ergast.com/api/f1/{}/{}/{}.json?limit=10000&offset=0'
ERGAST_ENDPOINT = 'file:/home/cloudera/ergast-f1-analysis-project/layers/data-acquisition/mock-endpoint/{}/{}/{}.json'

HDFS_USER = 'cloudera'
HDFS_ENDPOINT = 'http://localhost:50070'
HDFS_LOGS_FILE = 'Logs/incremental_data.log'
HDFS_LAST_FETCHED_FILE = 'Logs/last_fetched_race'

FLUME_INPUT_DIR = '/home/ergast/input_files'

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

# API communication
def fetch(table, year, round):
	url = ERGAST_ENDPOINT.format(year, round, table)
	print(f'Fetching URL... {url}')
	response = urllib.request.urlopen(url).read()
	print(f'Received response of length {len(response)}')
	return json.loads(response)

print('Step 1')
try:
	print(f'Reading last fetched race from HDFS: {HDFS_LAST_FETCHED_FILE}')
	with client.read(HDFS_LAST_FETCHED_FILE) as reader:
		lines = reader.read().decode().split('\n')
	last_year = int(lines[0].rstrip())
	last_round = int(lines[1].rstrip())
	print(f'Last fetched race: {last_round}_{last_year}')
except Exception as e:
	log(f'Cannot read last fetched year & round: {e}')
	sys.exit()

print('Step 2')
def is_race_available(year, round):
	j = fetch('results', year, round)
	total = int(j['MRData']['total'])
	return total > 0

try:
	current_year = last_year
	current_round = last_round + 1
	if is_race_available(current_year, current_round):
		print(f'New race available: {current_round}_{current_year}')
	else:
		current_year = last_year + 1
		current_round = 1
		if is_race_available(current_year, current_round):
                	print(f'New race available: {current_round}_{current_year}')
		else:
			log(f'No new race available. Tried: {last_round + 1}_{last_year} and 1_{last_year + 1}')
			sys.exit()
except Exception as e:
	log.error(f'Cannot check if there is new race: {e}')
	sys.exit()

print('Step 3')
try:
	with client.write(HDFS_LAST_FETCHED_FILE, overwrite=True) as writer:
                writer.write(str.encode(f'{current_year}\n{current_round}'))
	print(f'Updated last fetched race to {current_round}_{current_year}')
except Exception as e:
	log(f'Cannot update last fetched race: {e}')
	sys.exit()

print('Step 4-5')
try:
	results = fetch('results', current_year, current_round)
	laps = fetch('laps', current_year, current_round)
except Exception as e:
	log(f'Cannot fetch new data: {e}')
	sys.exit()

print('Step 6')
try:
	assert 'MRData' in results
	assert 'RaceTable' in results['MRData']
	assert int(results['MRData']['total']) > 0
	assert len(results['MRData']['RaceTable']['Races']) == 1
	assert 'circuitId' in results['MRData']['RaceTable']['Races'][0]['Circuit']
	print(f'Results validated successfully')
except Exception as e:
	log(f'Invalid JSON received from API (results, {current_year}, {current_round}): {e}')
	sys.exit()

try:
	assert 'MRData' in laps 
	assert 'RaceTable' in laps['MRData']
	assert int(laps['MRData']['total']) > 0
	assert len(laps['MRData']['RaceTable']['Races']) == 1
	assert len(laps['MRData']['RaceTable']['Races'][0]['Laps']) > 0
	assert 'driverId' in laps['MRData']['RaceTable']['Races'][0]['Laps'][0]['Timings'][0]
	assert 'position' in laps['MRData']['RaceTable']['Races'][0]['Laps'][0]['Timings'][0]
	print(f'Laps validated successfully')
except Exception as e:
	log(f'Invalid JSON received from API (laps, {current_year}, {current_round}): {e}')
	sys.exit()

print('Step 7-8')
try:
	filename = os.path.join(FLUME_INPUT_DIR, f'results_{current_round}_{current_year}.json')
	with open(filename, 'w+') as f:
		json.dump(results, f)
	log(f'Successfully saved results of round {current_round} in {current_year} to {filename}')

	filename = os.path.join(FLUME_INPUT_DIR, f'laps_{current_round}_{current_year}.json')
	with open(filename, 'w+') as f:
		json.dump(laps, f)
	log(f'Successfully saved laps of round {current_round} in {current_year} to {filename}')
except Exception as e:
	log(f'Cannot save valid JSON to Flume input dir: {e}')
