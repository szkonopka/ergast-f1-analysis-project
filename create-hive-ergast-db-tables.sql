CREATE TABLE ergast_results.driver_race(
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
);

CREATE TABLE ergast_results.race_season(
  raceSeasonID int,
  year Date,
  circuit varchar(100),
  winnerDriverExperience int,
  gapWinnerDriver int,
  numberAccident int
);


