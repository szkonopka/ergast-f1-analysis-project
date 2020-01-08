set hive.execution.engine=mr;

-- Load circuits.csv to temporary circuits_csv table with proper config, then to original circuits table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.circuits_csv
(circuitId varchar(50), name varchar(100))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/circuits.csv' INTO TABLE ergast_results.circuits_csv;
INSERT INTO TABLE ergast_results.circuits (circuitid, name)
SELECT st.circuitid, st.name 
FROM ergast_results.circuits_csv st
WHERE NOT EXISTS (SELECT 1
    FROM ergast_results.circuits tt
    WHERE tt.circuitid = st.circuitid
    AND tt.name = st.name)

-- Load races.csv to temporary races_csv table with proper config, then to original races table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.races_csv
(raceId int, year int, circuitId varchar(50))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/races.csv' INTO TABLE ergast_results.races_csv;
INSERT INTO TABLE ergast_results.races (raceid, year, circuitid)
SELECT DISTINCT st.raceid, st.year, st.circuitid
FROM ergast_results.races_csv st
WHERE NOT EXISTS (SELECT 1
    FROM ergast_results.races tt
    WHERE tt.raceid = st.raceid
    AND tt.year = st.year
    AND tt.circuitid = st.circuitid);

-- Load lapTimes.csv to temporary lapTimes_csv table with proper config, then to original lapTimes table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.lapTimes_csv
(raceId int, lap int, driverId varchar(50), position int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/lap_times.csv' INTO TABLE ergast_results.lapTimes_csv;
INSERT INTO TABLE ergast_results.lapTimes (raceid, lap, driverid, position)
SELECT DISTINCT st.raceid, st.lap, st.driverid, st.position
FROM ergast_results.lapTimes_csv st
WHERE NOT EXISTS (SELECT 1 
    FROM ergast_results.lapTimes tt
    WHERE tt.raceid = st.raceid
    AND tt.lap = st.lap
    AND tt.driverid = st.driverid
    AND tt.position = st.position);

-- Load drivers.csv to temporary drivers_csv table with proper config, then to original drivers table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.drivers_csv
(driverId varchar(50), forename varchar(50), surname varchar(50), nationality varchar(20))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/driver.csv' INTO TABLE ergast_results.drivers_csv;
INSERT INTO TABLE ergast_results.drivers (driverid, forename, surname, nationality)
SELECT DISTINCT st.driverid, st.forename, st.surname, st.nationality
FROM ergast_results.drivers_csv st
WHERE NOT EXISTS (SELECT 1
    FROM ergast_results.drivers tt
    WHERE tt.driverid = st.driverid
    AND tt.forename = st.forename
    AND tt.surname = st.surname
    AND tt.nationality = st.nationality);

-- Load results.csv to temporary results_csv table with proper config, then to original results table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.results_csv
(resultId int, raceId int, grid int, positionOrder int, statusId int, 
 points int, fastestLap float, fastestLapSpeed float, laps int, time float, driverId varchar(50), constructorId varchar(50))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/results.csv' INTO TABLE ergast_results.results_csv;
INSERT INTO TABLE ergast_results.results (resultid, raceid, grid, positionorder, statusid, 
 points, fastestlap, fastestlapspeed, laps, time, driverid, constructorid)
SELECT DISTINCT st.resultid, st.raceid, st.grid, st.positionorder, st.statusid, 
 st.points, st.fastestlap, st.fastestlapspeed, st.laps, st.time, st.driverid, st.constructorid
FROM ergast_results.results_csv st
WHERE NOT EXISTS (SELECT 1
    FROM ergast_results.results tt
    WHERE tt.resultid = st.resultid
    AND tt.raceid = st.raceid AND tt.grid = st.grid
    AND tt.positionorder = st.positionorder AND tt.statusid = st.statusid
    AND tt.points = st.points AND tt.fastestlap = st.fastestlap
    AND tt.fastestlapspeed = st.fastestlapspeed AND tt.laps = st.laps
    AND tt.time = st.time AND tt.driverid = st.driverid
    AND tt.constructorid = st.constructorid);

-- Load status.csv to temporary status_csv table with proper config, then to original status table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.status_csv 
(statusId int, status varchar(100))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/status.csv' INTO TABLE ergast_results.status_csv;
INSERT INTO TABLE ergast_results.status (statusid, status)
SELECT DISTINCT st.statusid, st.status
FROM ergast_results.status_csv st
WHERE NOT EXISTS (SELECT 1
    FROM ergast_results.status tt
    WHERE tt.statusid = st.statusid
    AND tt.status = st.status);

-- Load constructors.csv to temporary constructors_csv table with proper config, then to original constructors table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.constructors 
(constructorId varchar(50), name varchar(50), nationality varchar(20))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/constructors.csv' INTO TABLE ergast_results.constructors_csv;
INSERT INTO TABLE ergast_results.constructors (constructorid, name, nationality)
SELECT DISTINCT st.constructorid, st.name, st.nationality
FROM ergast_results.constructors_csv st
WHERE NOT EXISTS (SELECT 1
    FROM ergast_results.constructors tt
    WHERE tt.constructorid = st.constructorid
    AND tt.name = st.name
    AND tt.nationality = st.nationality);
