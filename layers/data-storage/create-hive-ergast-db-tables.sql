set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
set hive.tez.java.opts="-server -Xmx512m -Djava.net.preferIPv4Stack=true";
set hive.tez.container.size=512;

CREATE TABLE IF NOT EXISTS ergast_results.circuits(
  circuitId int,
  name varchar(100)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.races(
  raceId int,
  circuitId int,
  year Date
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.lapTimes(
  raceId int,
  driverId int,
  lap int,
  position int
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.drivers(
  driverId int,
  driverRef varchar(4),
  forename varchar(50),
  surname varchar(50),
  nationality varchar(20)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.results(
  resultId int,
  raceId int,
  grid int,
  driverId int,
  positionOrder int,
  statusId int,
  fastestLap int,
  fastestLatSpeed float,
  points int,
  laps int,
  rank int,
  constructorId int,
  time float
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.status(
  statusId int,
  status varchar(100)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.constructors(
  constructorId int,
  constructorRef varchar(10),
  name varchar(50),
  nationality varchar(20)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.driver_race(
  driverRaceID int,
  driverName varchar(50),
  constructor varchar(20),
  overtakeCount int,
  positionOrder int,
  status varchar(50),
  nationality varchar(20),
  fastestLapTime float,
  fastestLapSpeed float,
  time float,
  rank int,
  points int,
  laps int,
  raceSeasonID int
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_results.race_season(
  raceSeasonID int,
  year Date,
  circuit varchar(100),
  winnerDriverExperience int,
  gapWinnerDriver int,
  numberAccident int
) STORED AS ORC;


