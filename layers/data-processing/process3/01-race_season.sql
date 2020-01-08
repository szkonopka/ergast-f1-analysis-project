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
    
SELECT COUNT(*), s.status, IF(s.status IN (
            'Accident', 'Collision', 'Engine', 
            'Gearbox', 'Transmission', 'Clutch', 'Electrics', 
            'Hydraulics', 'Electrical', 'Spun off', 'Radiator', 
            'Suspension', 'Differential', 'Overheating', 'Mechanical', 
            'Tyre', 'Driver Seat', 'Puncture', 'Driveshaft'
        ), 1, 0)
FROM results res
JOIN status s ON s.statusid = res.statusid
GROUP BY s.status;
    
--
-- Insert to final table
--

INSERT INTO TABLE race_season
    SELECT 
        r.raceid AS raceseasonid, 
        r.year AS `year`, 
        c.name AS circuit, 
        0 AS winnerdriverexperience, --todo
        0 AS gapwinnerdriver, --todo
        SUM(na.accidents) AS numberaccident
    FROM races r
    JOIN circuits c ON r.circuitid = c.circuitid
    JOIN helper_numberaccident na ON r.raceid = na.raceid
    GROUP BY r.raceid, r.year, c.name;

--
-- Clean up helper tables
--

DROP TABLE helper_numberaccident;

--
-- Display result
--

SELECT * FROM race_season;

