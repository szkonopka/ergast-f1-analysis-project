set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
set hive.tez.java.opts="-server -Xmx512m -Djava.net.preferIPv4Stack=true";
set hive.tez.container.size=512;

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
