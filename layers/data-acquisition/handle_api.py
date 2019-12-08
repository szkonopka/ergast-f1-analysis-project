import urllib.request
import json
import logging
import sys
import os

# Configure logging
logger = logging.getLogger('ergast_handle_api')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('ergast_handle_api.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

# Constants
ENDPOINT = 'http://ergast.com/api/f1/{}/{}/{}.json?limit=10000&offset=0'
LAST_FETCHED_FILE = 'last_fetched_race'
FLUME_INPUT_DIR = '/home/ergast/input_files'

def fetch(table, year, round):
	response = urllib.request.urlopen(ENDPOINT.format(year, round, table))
	return json.loads(response.read())

logger.debug('Step 1')
try:
	f = open(LAST_FETCHED_FILE, 'r')
	lines = f.readlines()
	last_year = int(lines[0].rstrip())
	last_round = int(lines[1].rstrip())
except Exception as e:
	logger.error(f'Cannot read last fetched year & round: {e}')
	sys.exit()
finally:
	f.close()

logger.debug('Step 2')
try:
	j = fetch('results', last_year, last_round+1)
	total = int(j['MRData']['total'])
	if total == 0:
		j = fetch('results', last_year+1, 1)
		total = int(j['MRData']['total'])
		if total == 0:
			logger.info('There is no new race')
			sys.exit()
		else:
			current_year = last_year
			current_round = 1
	else:
		current_year = last_year
		current_round = last_round + 1	
except Exception as e:
	logger.error(f'Cannot check if there is new race: {e}')
	sys.exit()

logger.debug('Step 3')
try:
	f = open(LAST_FETCHED_FILE, 'w')
	f.write(f'{current_year}\n{current_round}')
except Exception as e:
	logger.error(f'Cannot update last fetched year: {e}')
	sys.exit()
finally:
	f.close()

logger.debug('Step 4-5')
try:
	results = fetch('results', current_year, current_round)
	laps = fetch('laps', current_year, current_round)
except Exception as e:
	logger.error(f'Cannot fetch new data: {e}')
	sys.exit()

logger.debug('Step 7')
try:
	assert 'MRData' in results
	assert 'RaceTable' in results['MRData']
	assert int(results['MRData']['total']) > 0
	assert len(results['MRData']['RaceTable']['Races']) == 1
	assert 'circuitId' in results['MRData']['RaceTable']['Races'][0]['Circuit']
except Exception as e:
	logger.error(f'Invalid JSON received from API (results, {current_year}, {current_round}): {e}')
	sys.exit()

try:
	assert 'MRData' in laps 
	assert 'RaceTable' in laps['MRData']
	assert int(laps['MRData']['total']) > 0
	assert len(laps['MRData']['RaceTable']['Races']) == 1
	assert len(laps['MRData']['RaceTable']['Races'][0]['Laps']) > 0
	assert 'driverId' in laps['MRData']['RaceTable']['Races'][0]['Laps'][0]['Timings'][0]
	assert 'position' in laps['MRData']['RaceTable']['Races'][0]['Laps'][0]['Timings'][0]
except Exception as e:
	logger.error(f'Invalid JSON received from API (laps, {current_year}, {current_round}): {e}')
	sys.exit()

logger.debug('Step 8-9')
try:
	filename = os.path.join(FLUME_INPUT_DIR, f'results_{current_round}_{current_year}.json')
	with open(filename, 'w+') as f:
		json.dump(results, f)
	logger.info(f'Successfuly saved results to {filename}')

	filename = os.path.join(FLUME_INPUT_DIR, f'laps_{current_round}_{current_year}.json')
	with open(filename, 'w+') as f:
		json.dump(laps, f)
	logger.info(f'Successfuly saved laps to {filename}')
except Exception as e:
	logger.error(f'Cannot save valid JSON to Flume input dir: {e}')
