set hive.execution.engine=mr;
set hive.auto.convert.join=false;

--
-- Create helper tables
--

CREATE TABLE IF NOT EXISTS helper_lap_positions(
    raceid int,
    driverid varchar(50),
    lap int,
    position int
);

TRUNCATE TABLE helper_lap_positions;

CREATE TABLE IF NOT EXISTS helper_overtake_counts(
    raceid int,
    driverid varchar(50),
    overtake_count int
);

TRUNCATE TABLE helper_overtake_counts;

--
-- Calculate transformations and insert to helper tables
--

INSERT INTO TABLE helper_lap_positions
    SELECT lt.raceid, lt.driverid, lt.lap, lt.position
    FROM laptimes lt;
   
INSERT INTO TABLE helper_overtake_counts 
    SELECT
        curr.raceid,
        curr.driverid,
        SUM(IF(next.position < curr.position, curr.position - next.position, 0))
    FROM helper_lap_positions curr
    LEFT OUTER JOIN helper_lap_positions next ON (
        curr.raceid = next.raceid AND
        curr.driverid = next.driverid AND
        curr.lap + 1 = next.lap
    )
    GROUP BY curr.raceid, curr.driverid;
    
--
-- Insert to final table
--

INSERT INTO TABLE driver_race
    SELECT
        res.resultid AS driverRaceID,
        CONCAT(d.forename, ' ', d.surname) AS driverName,
        MAX(res.constructorid) AS constructor,
        MAX(oc.overtake_count) AS overtakeCount,
        MAX(res.positionorder) AS positionOrder,
        MAX(s.status) AS status,
        MAX(d.nationality) AS nationality,
        MAX(res.fastestlap) AS fastestLapTime,
        MAX(res.fastestlatspeed) AS fastestLapSpeed,
        MAX(res.time) AS time,
        MAX(res.points) AS points,
        MAX(res.laps) AS laps,
        rs.raceseasonid AS raceSeasonID
    FROM results res
    JOIN drivers d ON res.driverid = d.driverid
    JOIN status s ON res.statusid = s.statusid
    JOIN helper_overtake_counts oc ON res.driverid = oc.driverid AND res.raceid = oc.raceid
    JOIN race_season rs ON res.raceid = rs.raceseasonid
    GROUP BY res.resultid, CONCAT(d.forename, ' ', d.surname), rs.raceseasonid;

--
-- Clean up helper tables
--

DROP TABLE helper_lap_positions;
DROP TABLE helper_overtake_counts;

--
-- Display final table
--

SELECT * FROM driver_race;

