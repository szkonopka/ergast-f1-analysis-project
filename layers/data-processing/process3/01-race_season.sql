set hive.execution.engine=mr;
set hive.auto.convert.join=false;

--
-- Create helper tables
--

CREATE TABLE IF NOT EXISTS helper_numberaccident(
    raceid int,
    accidents int
);

TRUNCATE TABLE helper_numberaccident;


CREATE TABLE IF NOT EXISTS helper_gapwinnerdriver(
    raceid int,
    gap int
);

TRUNCATE TABLE helper_gapwinnerdriver;

CREATE TABLE IF NOT EXISTS helper_winnerdriverexperience(
    driverid varchar(50),
    raceid int,
    wins int
);

TRUNCATE TABLE helper_winnerdriverexperience;

--
-- Calculate transformations and insert to helper tables
--

INSERT INTO TABLE helper_numberaccident
    SELECT 
        res.raceid,
        SUM(IF(s.status IN (
            'Accident', 'Collision', 'Engine', 
            'Gearbox', 'Transmission', 'Clutch', 'Electrics', 
            'Hydraulics', 'Electrical', 'Spun off', 'Radiator', 
            'Suspension', 'Differential', 'Overheating', 'Mechanical', 
            'Tyre', 'Driver Seat', 'Puncture', 'Driveshaft'
        ), 1, 0))
    FROM results res
    JOIN status s ON s.statusid = res.statusid
    GROUP BY res.raceid;
  
INSERT INTO TABLE helper_gapwinnerdriver
    SELECT
        res.raceid,
        ABS(res.positionorder - res.grid)
    FROM results res
    WHERE res.positionorder = 1;

    
INSERT INTO TABLE helper_winnerdriverexperience
    SELECT
        res.driverid,
        res.raceid,
        COUNT(*)
    FROM results res
    JOIN results prev ON res.driverid = prev.driverid
    WHERE prev.raceid < res.raceid AND prev.positionorder = 1 AND res.positionorder = 1
    GROUP BY res.driverid, res.raceid;
    
--
-- Insert to final table
--

INSERT INTO TABLE race_season
    SELECT 
        r.raceid AS raceseasonid, 
        r.year AS `year`, 
        c.name AS circuit, 
        NVL(MAX(wde.wins), 0) AS winnerdriverexperience,
        MAX(gwd.gap) AS gapwinnerdriver,
        MAX(na.accidents) AS numberaccident
    FROM races r
    JOIN circuits c ON r.circuitid = c.circuitid
    LEFT JOIN helper_winnerdriverexperience wde ON r.raceid = wde.raceid
    LEFT JOIN helper_gapwinnerdriver gwd ON r.raceid = gwd.raceid
    JOIN helper_numberaccident na ON r.raceid = na.raceid
    GROUP BY r.raceid, r.year, c.name;

--
-- Clean up helper tables
--

DROP TABLE helper_winnerdriverexperience;
DROP TABLE helper_gapwinnerdriver;
DROP TABLE helper_numberaccident;

--
-- Display result
--

SELECT * 
FROM race_season rs
ORDER BY rs.raceseasonid DESC;

