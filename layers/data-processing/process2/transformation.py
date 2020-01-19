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
HDFS_DATA_SOURCE = './Data_source'
RACE_ID_MULTIPLIER = 100
DELIM = ","

class Lap:
    def __init__(self, raceId, lap, driverId, position):
        self.raceId = raceId
        self.lap = lap
        self.driverId = driverId
        self.position = position

    def get_csv_row(self):
        return str(self.raceId) + DELIM + str(self.lap) + DELIM + str(self.driverId) + DELIM + str(self.position)

class Race:
    def __init__(self, raceId, year, circuitId):
        self.raceId = raceId
        self.year = year
        self.circuitId = circuitId

    def get_csv_row(self):
        return str(self.raceId) + DELIM + str(self.year) + DELIM + str(self.circuitId)

class Circuit:
    def __init__(self, circuitId, name):
        self.circuitId = circuitId
        self.name = name

    def get_csv_row(self):
        return str(self.circuitId) + DELIM + str(self.name)

class Result:
    def __init__(self, raceId, grid, driverId, positionorder, statusId, points, fastestLap, fastestLapSpeed):
        self.raceId = raceId
        self.grid = grid
        self.driverId = driverId
        self.positionorder = positionorder
        self.statusId = statusId
        self.points = points
        self.fastestLap = fastestLap
        self.fastestLapSpeed = fastestLapSpeed

    def get_csv_row(self):
        return str(self.raceId) + DELIM + str(self.grid) + DELIM + str(self.driverId) + DELIM + str(self.positionorder) + DELIM + str(self.statusId) + DELIM + str(self.points) + DELIM + str(self.fastestLap) + DELIM + str(self.fastestLapSpeed)

class Driver:
    def __init__(self, driverId, forename, surname, nationality):
        self.driverId = driverId
        self.forename = forename
        self.surname = surname
        self.nationality = nationality

    def get_csv_row(self):
        return str(self.driverId) + DELIM + str(self.forename) + DELIM + str(self.surname) + DELIM + str(self.nationality)

class Status:
    def __init__(self, statusId, status):
        self.statusId = statusId
        self.status = status

    def get_csv_row(self):
        return str(self.statusId) + DELIM + str(self.status)

class Constructor:
    def __init__(self, constructorId, name, nationality):
        self.constructorId = constructorId
        self.name = name
        self.nationality = nationality

    def get_csv_row(self):
        return str(self.constructorId) + DELIM + str(self.name) + DELIM + str(self.nationality)

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
    data = None
    # This is just for local testing
    """
    with open(path, 'r') as json_file:
        data = json.load(json_file)
    """
    with client.read(path) as json_file:
        data = json.load(json_file)
    
    return data

def save_vector_to_csv(path, data):
    try:
        for element in data:
            save_to_csv(path, element.get_csv_row())
        return True
    except Exception as e:
        print(f'Something went not well during csv saving: {e}')
        return False

def save_to_csv(path, csv_row):
    # This is just for local testing
    """
    with open(path, 'a') as writer:
        writer.write(str.encode(csv_row))
        writer.write(str.encode('\n'))
    """
    with client.write(path, append=True) as writer:
        writer.write(str.encode(csv_row))
        writer.write(str.encode('\n'))
    

def process_results(filename):
    races = []
    circuits = []
    results = []
    drivers = []
    constructors = []
    statuses = []

    data = get_json(HDFS_NEW_INCREMENTAL_DATA + "/Results/" + filename)
    statusId = 0
    root = data["MRData"]["RaceTable"]
    season = root["season"]
    for race in root["Races"]:
        roundId = race["round"]
        year = race["season"]
        circuit = race["Circuit"]
        circuitId = circuit["circuitId"]
        circuitName = circuit["circuitName"]

        circuits.append(Circuit(circuitId, circuitName))
        races.append(Race(int(roundId)+ int(season) * RACE_ID_MULTIPLIER, year, circuitId))

        for result in race["Results"]:
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

            fastestLap = result["FastestLap"]

            time_obj = fastestLap["Time"]
            time = time_obj["time"]

            lap = fastestLap["lap"]
            average_speed = fastestLap["AverageSpeed"]
            speed = average_speed["speed"]

            if status not in statuses:
                statuses.append(Status(statusId, status))
                statusId = statusId + 1

            results.append(Result(int(roundId) + int(season) * RACE_ID_MULTIPLIER, grid, driverId, position, statusId, points, lap, speed))
            drivers.append(Driver(driverId, forename, surname, driver_nationality))
            constructors.append(Constructor(constructorId, name, constructor_nationality))
    
    processingResult = True
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/races.csv", races)
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/circuits.csv", circuits)
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/results.csv", results)
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/driver.csv", drivers)
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/constructors.csv", constructors)
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/status.csv", statuses)
    return processingResult

def process_laps(filename):
    laps = []
    data = get_json(HDFS_NEW_INCREMENTAL_DATA + "/Lap_times/" + filename)

    root = data["MRData"]["RaceTable"]
    for race in root['Races']:
       raceId = int(race["season"]) * RACE_ID_MULTIPLIER + int(race["round"])
       for lap in race["Laps"]:
           number = lap["number"]
           for timing in lap["Timings"]:
               position = timing["position"]
               driverId = timing["driverId"]
               laps.append(Lap(raceId, number, driverId, position))

    processingResult = True
    processingResult &= save_vector_to_csv(HDFS_DATA_SOURCE + "/laps.csv", laps)
    return processingResult

filename = sys.argv[1]

results_pattern = r'results\_.*\_.*\.json*'
lap_times_pattern = r'lapTimes\_.*\_.*\.json*'

if ('results' in filename):
    print("Processing results file: {}".format(filename))
    if process_results(filename):
        print(f'{filename} has been processed succesfully, remove it from system')
        os.system("hdfs dfs -rm {}".format(HDFS_NEW_INCREMENTAL_DATA + "/Results/" + filename))
    else:
        print(f'{filename} has not been processed succesfully, check its format')
elif ('lapTimes' in filename):
    print("Processing lapTimes file: {}".format(filename))
    if process_laps(filename):
        print(f'{filename} has been processed succesfully, remove it from system')
        os.system("hdfs dfs -rm {}".format(HDFS_NEW_INCREMENTAL_DATA + "/Lap_times/" + filename))
    else:
        print(f'{filename} has not been processed succesfully, check its format')
else:
    print("Invalid argument")
