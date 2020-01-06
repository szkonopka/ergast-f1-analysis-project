set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
set hive.tez.java.opts="-server -Xmx512m -Djava.net.preferIPv4Stack=true";
set hive.tez.container.size=512;

CREATE TABLE IF NOT EXISTS ergast_results.circuits(
  circuitId varchar(50),
  name varchar(100)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.races(
  raceId int,
  circuitId varchar(50),
  year int
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.lapTimes(
  raceId int,
  driverId varchar(50),
  lap int,
  position int
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.drivers(
  driverId varchar(50),
  forename varchar(50),
  surname varchar(50),
  nationality varchar(20)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.results(
  resultId int,
  raceId int,
  grid int,
  driverId varchar(50),
  positionOrder int,
  statusId int,
  fastestLap int,
  fastestLatSpeed float,
  points int,
  laps int,
  rank int,
  constructorId varchar(50),
  time float
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.status(
  statusId int,
  status varchar(100)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.constructors(
  constructorId varchar(50),
  name varchar(50),
  nationality varchar(20)
) STORED AS ORC;

