--
-- Initialize tables
--

set hive.execution.engine=tez;

-- Result table

CREATE TABLE IF NOT EXISTS driver_race(
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
  points int,
  laps int,
  raceSeasonID int
) STORED AS ORC;

TRUNCATE TABLE driver_race;

-- Partial (helper) table

CREATE TABLE IF NOT EXISTS partial_overtakecount(
    driverraceid int,
    overtakecount int
);
TRUNCATE TABLE partial_overtakecount;

