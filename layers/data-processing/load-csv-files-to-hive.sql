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
INSERT INTO TABLE ergast_results.circuits SELECT * FROM ergast_results.circuits_csv;

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
INSERT INTO TABLE ergast_results.races SELECT * FROM ergast_results.races_csv;

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
INSERT INTO TABLE ergast_results.lapTimes SELECT * FROM ergast_results.lapTimes_csv;

-- Load drivers.csv to temporary drivers_csv table with proper config, then to original drivers table

CREATE EXTERNAL TABLE IF NOT EXISTS ergast_results.drivers_csv
(driverId varchar(50), forename varchar(50), surname varchar(50), nationality varchar(20))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/Data_source/driver.csv' INTO TABLE ergast_results.drivers_csv;
INSERT INTO TABLE ergast_results.drivers SELECT * FROM ergast_results.drivers_csv;

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
INSERT INTO TABLE ergast_results.results SELECT * FROM ergast_results.results_csv;

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
INSERT INTO TABLE ergast_results.status SELECT * FROM ergast_results.status_csv;

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
INSERT INTO TABLE ergast_results.constructors SELECT * FROM ergast_results.constructors_csv;
