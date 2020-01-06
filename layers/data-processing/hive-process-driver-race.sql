--
-- Calculate transformations and insert to helper tables
--

set hive.execution.engine=mr;

INSERT INTO TABLE partial_overtakecount
    SELECT 
        r.raceid AS driverraceid,
        5 AS overtakecount -- todo
    FROM races r;

--
-- Insert to result table
--

INSERT INTO TABLE driver_race
    SELECT
        res.resultid AS driverRaceID,
        CONCAT(d.forename, ' ', d.surname) AS driverName,
        res.constructorid AS constructor,
        oc.overtakecount AS overtakeCount,
        res.positionorder AS positionOrder,
        s.status AS status,
        d.nationality AS nationality,
        res.fastestlap AS fastestLapTime,
        res.fastestlatspeed AS fastestLapSpeed,
        res.time AS time,
        res.points AS points,
        res.laps AS laps,
        rs.raceseasonid AS raceSeasonID
    FROM results res
    JOIN drivers d ON res.driverid = d.driverid
    JOIN status s ON res.statusid = s.statusid
    JOIN partial_overtakecount oc ON res.resultid = oc.driverraceid
    JOIN race_season rs ON res.raceid = rs.raceseasonid;

--
-- Display result
--

SELECT * FROM driver_race;

