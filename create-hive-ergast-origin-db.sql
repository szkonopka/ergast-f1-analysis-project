set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
set hive.tez.java.opts="-server -Xmx512m -Djava.net.preferIPv4Stack=true";
set hive.tez.container.size=512;

CREATE TABLE IF NOT EXISTS ergast_origin.circuits(
  circuitId int,
  name varchar(100)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_origin.races(
  raceId int,
  circuitId int,
  year int
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_origin.lapTimes(
  raceId int,
  driverId int,
  lap int,
  position int
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_origin.drivers(
  driverId int,
  driverRef varchar(4),
  forename varchar(50),
  surname varchar(50),
  nationality varchar(20)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_origin.results(
  resultId int,
  raceId int,
  grid int,
  driverId int,
  positionOrder int,
  statusId,
  fastestLap int,
  fastestLatSpeed float,
  points int,
  laps int,
  rank int,
  constructorId int,
  time float
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_origin.status(
  statusId int,
  status varchar(100)
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ergast_origin.constructors(
  constructorId int,
  constructorRef varchar(10),
  name varchar(50),
  nationality varchar(20)
) STORED AS ORC;



