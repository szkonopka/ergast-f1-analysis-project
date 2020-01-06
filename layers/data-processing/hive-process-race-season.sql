--
-- Calculate transformations and insert to helper tables
--

set hive.execution.engine=mr;

-- Transformation 2

INSERT INTO TABLE partial_winnerdriverexperience
    SELECT 
        r.raceid AS raceid,
        0 AS winnerdriverexperience -- todo
    FROM races r;
    
-- Transformation 3

INSERT INTO TABLE partial_gapwinnerdriver
    SELECT 
        r.raceid AS raceid,
        0 AS gapwinnerdriver -- todo
    FROM races r;

-- Transformation 4

INSERT INTO TABLE partial_numberaccident
    SELECT 
        res.raceid AS raceid,
        SUM(IF(s.status IN (
            'Accident', 'Collision', 'Engine', 
            'Gearbox', 'Transmission', 'Clutch', 'Electrics', 
            'Hydraulics', 'Electrical', 'Spun off', 'Radiator', 
            'Suspension', 'Differential', 'Overheating', 'Mechanical', 
            'Tyre', 'Driver Seat', 'Puncture', 'Driveshaft'
        ), 1, 0)) AS numberaccident
    FROM results res
    JOIN status s ON s.statusid = res.statusid
    GROUP BY res.raceid;

--
-- Insert to result table
--

INSERT INTO TABLE race_season
    SELECT 
        r.raceid AS raceseasonid, 
        r.year AS `year`, 
        c.name AS circuit, 
        SUM(wde.winnerdriverexperience) AS winnerdriverexperience,
        SUM(gwd.gapwinnerdriver) AS gapwinnerdriver,
        SUM(na.numberaccident) AS numberaccident
    FROM races r
    JOIN circuits c ON r.circuitid = c.circuitid
    JOIN partial_winnerdriverexperience wde ON r.raceid = wde.raceid
    JOIN partial_gapwinnerdriver gwd ON r.raceid = gwd.raceid
    JOIN partial_numberaccident na ON r.raceid = na.raceid
    GROUP BY r.raceid, r.year, c.name;

--
-- Display result
--

SELECT * FROM race_season;
