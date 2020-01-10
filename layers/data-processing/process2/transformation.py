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
RACE_ID_MULTIPLIER = 100
DELIM = ","

class Lap:
	def __init__(raceId, lap, driverId, position):
		self.raceId = raceId
		self.lap = lap
		self.driverId = driverId
        self.position = position

    def get_csv_row():
        return self.driverId + DELIM + self.position + DELIM + self.time

class Race:
    def __init__(raceId, year, circuitId):
        self.raceId = raceId
        self.year = year
        self.circuitId = circuitId

    def get_csv_row():
        return self.raceId + DELIM + self.year + DELIM + self.circuitId

class Circuit:
    def __init__(circuitId, name):
        self.circuitId = circuitId
        self.name = name

    def get_csv_row():
        return self.circuitId + DELIM + self.name

class Result:
    def __init__(raceId, grid, driverId, positionorder, statusId, points, fastestLap, fastestLapSpeed):
        self.raceId = raceId
        self.grid = grid
        self.driverId = driverId
        self.positionorder = positionorder
        self.statusId = statusId
        self.points = points
        self.fastestLap = fastestLap
        self.fastestLapSpeed = fastestLapSpeed

    def get_csv_row():
        return self.raceId + DELIM + self.grid + DELIM + self.driverId + DELIM + self.positionorder  + DELIM +
            self.statusId + DELIM + self.points + DELIM + self.fastestLap + DELIM + self.fastestLapSpeed

class Driver:
    def __init__(driverId, forename, surname, nationality):
        self.driverId = driverId
        self.forename = forename
        self.surname = surname
        self.nationality = nationality

    def get_csv_row():
        return self.driverId + DELIM + self.forename + self.surname + self.nationality

class Status:
    def __init__(statusId, status):
        self.statusId = statusId
        self.status = status

    def get_csv_row():
        return self.statusId + DELIM + self.status

class Constructor:
    def __init__(constructorId, name, nationality):
        self.constructorId = constructorId
        self.name = name
        self.nationality = nationality

    def get_csv_row():
        return self.constructorId + DELIM + self.name + DELIM + self.nationality

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

def save_vector_to_csv(path, data):
    for element in data:
        save_to_csv(path, element.get_csv_row())

def save_to_csv(path, csv_row):
	with client.write(path, append=True) as write:
		writer.write(str.encode(csv_data))

def process_results(filename):
    races = []
    circuits = []
    results = []
    driver = []
    constructors = []
    statuses = []

	data = get_json(HDFS_NEW_INCREMENTAL_DATA + "/Results/" + filename)
    statusId = 0
    root = data["MRData"]["RaceTable"]
    season = root["season"]
	for race in data["Races"]
		roundId = race["round"]
        year = race["season"]
        circuit = race["Circuit"]
        circuitId = circuit["circuitId"]
        circuitName = circuit["circuitName"]

        circuits.append(Circuit(circuitId, circuitName))
        races.append(Race((round + season) * RACE_ID_MULTIPLIER, year, circuitId))

        for result in race["Results"]
            position = result["position"]
            points = result["points"]

            driver = result["Driver"]
            driverId = driver["driverId"]
            forename = driver["givenName"]
            surname = driver["familyName"]
            driver_nationality = driver["nationality"]

            constructor = result["Constructor"]
            constructorId = constructor["constructorId"]
            name = constructor["name"]
            constructor_nationality = constructor["nationality"]

            status = result["status"]
            laps_param = result["laps"]
            grid = result["grid"]

            time_obj = result["Time"]
            time = time_obj["time"]

            fastestLap = result["FastestLap"]
            lap = fastestLap["lap"]
            average_speed = fastestLap["AverageSpeed"]
            speed = average_speed["speed"]

            if status not in statuses:
                statuses.append(Status(statusId, status))
                statusId = statusId + 1

            results.append(Result((round + season) * RACE_ID_MULTIPLIER, grid, driverId, position, statusId, points, lap, speed))
            driver.append(Driver(driverId, forename, surname, driver_nationality))
            constructors.append(Constructor(constructorId, name, constructor_nationality))

    save_vector_to_csv(HDFS_DATA_SOURCE + "/races.csv", races)
    save_vector_to_csv(HDFS_DATA_SOURCE + "/circuits.csv", circuits)
    save_vector_to_csv(HDFS_DATA_SOURCE + "/results.csv", results)
    save_vector_to_csv(HDFS_DATA_SOURCE + "/driver.csv", driver)
    save_vector_to_csv(HDFS_DATA_SOURCE + "/constructors.csv", constructors)
    save_vector_to_csv(HDFS_DATA_SOURCE + "/status.csv", statuses)

def process_laps(filename):
	laps = []
	data = get_json(HDFS_NEW_INCRMENTAL_DATA + "/Laps/" + filename)

    root = data["MRData"]["RaceTable"]
	for race in root['Races']
        raceId = (int(race["season"]) + int(race["round"])) * RACE_ID_MULTIPLIER
        for lap in race["Laps"]
            lap = lap["number"]
            for timing in lap["Timings"]
                laps.add(Lap(raceId, lap, timing["driverId"], timing["position"]))

    save_vector_to_csv(HDFS_DATA_SOURCE + "/laps.csv", laps)

filename = sys.argv[1]

results_pattern = r'results\_.*\_.*\.json*'
lapTimes_pattern = r'lapTimes\_.*\_.*\.json*'

if (re.match(results_pattern, filename)):
	process_results(filename)
else if (re.match(lap_times_pattern, filename)):
	process_laps(filename)
else:
	print("Invalid argument")
