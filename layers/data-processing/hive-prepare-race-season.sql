--
-- Initialize tables
--

set hive.execution.engine=tez;

-- Result table

CREATE TABLE IF NOT EXISTS race_season(
  raceSeasonID int,
  year int,
  circuit varchar(100),
  winnerDriverExperience int,
  gapWinnerDriver int,
  numberAccident int
) STORED AS ORC;

TRUNCATE TABLE race_season;

-- Partial (helper) tables

CREATE TABLE IF NOT EXISTS partial_winnerdriverexperience(
    raceid int,
    winnerdriverexperience int
);

TRUNCATE TABLE partial_winnerdriverexperience;

CREATE TABLE IF NOT EXISTS partial_gapwinnerdriver(
    raceid int,
    gapwinnerdriver int
);

TRUNCATE TABLE partial_gapwinnerdriver;

CREATE TABLE IF NOT EXISTS partial_numberaccident(
    raceid int,
    numberaccident int
);

TRUNCATE TABLE partial_numberaccident;
