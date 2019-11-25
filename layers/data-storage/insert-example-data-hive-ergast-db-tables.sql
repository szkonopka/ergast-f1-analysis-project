set hive.execution.engine=mr;
INSERT INTO TABLE status
	VALUES (1, 'Finished'), (2, 'Disqualified'), (3, 'Accident'), (4, 'Collision'), (5, 'Engine'), (6, 'Gearbox'), (7, 'Transmission'), (8, 'Clutch'), (9, 'Hydraulics'), (10, 'Electrical'), (11, 'Power Unit'), (12, 'Collision damage'), (13, 'Brake duct'), (14, 'Seat'), (15, 'Damage');

INSERT INTO TABLE circuits
	VALUES ('monaco', 'Circuit de Monaco'), ('hockenheimring', 'Hockenheimring'), ('bahrain', 'Bahrain International Circuit'), ('monza', 'Autodromo Nazionale di Monza'), ('suzuka','Suzuka Circuit'), ('sochi','Sochi Autodrom'), ('red_bull_ring','Red Bull Ring'), ('hungaroring','Hungaroring'), ('silverstone','Silverstone Circuit'), ('albert_park','Albert Park Grand Prix Circuit');
	
INSERT INTO TABLE constructors
	VALUES ('mclaren', 'McLaren', 'British'), ('williams','Williams','British'), ('renault','Renault','French'), ('ferrari','Ferrari','Italian'), ('toro_rosso','Toro Rosso','Italian'), ('force_india','Force India','Indian'), ('alfa','Alfa Romeo','Italian'), ('haas','Haas F1 Team','American'), ('racing_point','Racing Point','British'), ('mercedes','Mercedes','German');
	
INSERT INTO TABLE races
	VALUES (1, 'monaco', 2019), (2,'hockenheimring', 2019), (3,'bahrain',2019), (4,'monza',2019), (5,'suzuka',2019), (6,'sochi',2019), (7,'red_bull_ring',2019), (8,'hungaroring',2019), (9,'silverstone',2019), (10,'albert_park',2019);
	
INSERT INTO TABLE drivers
	VALUES ('hamilton','Lewis','Hamilton','British'), ('rosberg','Nico','Rosberg','German'), ('raikkonen','Kimi','Raikkonen','Finnish'), ('kubica','Robert','Kubica','Polish'), ('vettel','Sebastian','Vettel','German'), ('max_verstappen','Max','Verstappen','Dutch'), ('grosjean','Romain','Grosjean','French'), ('stroll','Lance','Stroll','Canadian'), ('giovinazzi','Antonio','Giovinazzi','Italian'), ('leclerc','Charles','Leclerc','Monegasque'), ('norris','Lando','Norris','British'), ('russell','George','Russell','British'), ('albon','Alexander','Albon','Thai'), ('sainz','Carlos','Sainz','Spanish');
	
INSERT INTO TABLE lapTimes
	VALUES (1, 'kubica', 1, 1), (1, 'kubica', 2, 1),(1, 'kubica', 3, 1),(1, 'kubica', 4, 1),(1, 'kubica', 5, 1),(1, 'kubica', 6, 1),(1, 'kubica', 7, 1),(1, 'kubica', 8, 1),(1, 'kubica', 9, 1),(1, 'kubica', 10, 1),(1, 'kubica', 11, 1),(1, 'kubica', 12, 1),(1, 'kubica', 13, 1),(1, 'kubica', 14, 1),(1, 'kubica', 15, 1),(1, 'kubica', 16, 1),(1, 'kubica', 17, 1),(1, 'kubica', 18, 1),(1, 'kubica', 19, 1),(1, 'kubica', 20, 1),(1, 'kubica', 21, 1),(1, 'kubica', 22, 1),(1, 'kubica', 23, 1),(1, 'kubica', 24, 1),(1, 'kubica', 25, 1),(1, 'kubica', 26, 1),(1, 'kubica', 27, 1),(1, 'kubica', 28, 1),(1, 'kubica', 29, 1),(1, 'kubica', 30, 1),
	(1, 'rosberg', 1, 2), (1, 'rosberg', 2, 3),(1, 'rosberg', 3, 3),(1, 'rosberg', 4, 3),(1, 'rosberg', 5, 3),(1, 'rosberg', 6, 2),(1, 'rosberg', 7, 2),(1, 'rosberg', 8, 2),(1, 'rosberg', 9, 2),(1, 'rosberg', 10, 2),(1, 'rosberg', 11, 2),(1, 'rosberg', 12, 2),(1, 'rosberg', 13, 2),(1, 'rosberg', 14, 2),(1, 'rosberg', 15, 2),(1, 'rosberg', 16, 2),(1, 'rosberg', 17, 2),(1, 'rosberg', 18, 2),(1, 'rosberg', 19, 2),(1, 'rosberg', 20, 2),(1, 'rosberg', 21, 4),(1, 'rosberg', 22, 4),(1, 'rosberg', 23, 4),(1, 'rosberg', 24, 4),(1, 'rosberg', 25, 4),(1, 'rosberg', 26, 4),(1, 'rosberg', 27, 4),(1, 'rosberg', 28, 4),(1, 'rosberg', 29, 4),(1, 'rosberg', 30, 4),
	(1, 'hamilton', 1, 3), (1, 'hamilton', 2, 2),(1, 'hamilton', 3, 2),(1, 'hamilton', 4, 2),(1, 'hamilton', 5, 2),(1, 'hamilton', 6, 5),(1, 'hamilton', 7, 5),(1, 'hamilton', 8, 5),(1, 'hamilton', 9, 5),(1, 'hamilton', 10, 5),(1, 'hamilton', 11, 5),(1, 'hamilton', 12, 5),(1, 'hamilton', 13, 5),(1, 'hamilton', 14, 5),(1, 'hamilton', 15, 5),(1, 'hamilton', 16, 5),(1, 'hamilton', 17, 5),(1, 'hamilton', 18, 5),(1, 'hamilton', 19, 5),(1, 'hamilton', 20, 5),(1, 'hamilton', 21, 3),(1, 'hamilton', 22, 3),(1, 'hamilton', 23, 3),(1, 'hamilton', 24, 3),(1, 'hamilton', 25, 3),(1, 'hamilton', 26, 3),(1, 'hamilton', 27, 3),(1, 'hamilton', 28, 3),(1, 'hamilton', 29, 3),(1, 'hamilton', 30, 3),
	(1, 'vettel', 1, 4), (1, 'vettel', 2, 4),(1, 'vettel', 3, 4),(1, 'vettel', 4, 4),(1, 'vettel', 5, 4),(1, 'vettel', 6, 3),(1, 'vettel', 7, 3),(1, 'vettel', 8, 3),(1, 'vettel', 9, 3),(1, 'vettel', 10, 3),(1, 'vettel', 11, 4),(1, 'vettel', 12, 4),(1, 'vettel', 13, 4),(1, 'vettel', 14, 4),(1, 'vettel', 15, 4),(1, 'vettel', 16, 4),(1, 'vettel', 17, 4),(1, 'vettel', 18, 4),(1, 'vettel', 19, 4),(1, 'vettel', 20, 4),(1, 'vettel', 21, 5),(1, 'vettel', 22, 5),(1, 'vettel', 23, 5),(1, 'vettel', 24, 5),(1, 'vettel', 25, 5),(1, 'vettel', 26, 5),(1, 'vettel', 27, 5),(1, 'vettel', 28, 5),(1, 'vettel', 29, 5),(1, 'vettel', 30, 5),
	(1, 'max_verstappen', 1, 5), (1, 'max_verstappen', 2, 5),(1, 'max_verstappen', 3, 5),(1, 'max_verstappen', 4, 5),(1, 'max_verstappen', 5, 5),(1, 'max_verstappen', 6, 4),(1, 'max_verstappen', 7, 4),(1, 'max_verstappen', 8, 4),(1, 'max_verstappen', 9, 4),(1, 'max_verstappen', 10, 4),(1, 'max_verstappen', 11, 3),(1, 'max_verstappen', 12, 3),(1, 'max_verstappen', 13, 3),(1, 'max_verstappen', 14, 3),(1, 'max_verstappen', 15, 3),(1, 'max_verstappen', 16, 3),(1, 'max_verstappen', 17, 3),(1, 'max_verstappen', 18, 3),(1, 'max_verstappen', 19, 3),(1, 'max_verstappen', 20, 3),(1, 'max_verstappen', 21, 2),(1, 'max_verstappen', 22, 2),(1, 'max_verstappen', 23, 2),(1, 'max_verstappen', 24, 2),(1, 'max_verstappen', 25, 2),(1, 'max_verstappen', 26, 2),(1, 'max_verstappen', 27, 2),(1, 'max_verstappen', 28, 2),(1, 'max_verstappen', 29, 2),(1, 'max_verstappen', 30, 2),
	(1, 'grosjean', 1, 6), (1, 'grosjean', 2, 6),(1, 'grosjean', 3, 6),(1, 'grosjean', 4, 6),(1, 'grosjean', 5, 6),(1, 'grosjean', 6, 6),(1, 'grosjean', 7, 6),(1, 'grosjean', 8, 6),(1, 'grosjean', 9, 6),(1, 'grosjean', 10, 6),(1, 'grosjean', 11, 6),(1, 'grosjean', 12, 6),(1, 'grosjean', 13, 6),(1, 'grosjean', 14, 6),(1, 'grosjean', 15, 6),(1, 'grosjean', 16, 6),(1, 'grosjean', 17, 6),(1, 'grosjean', 18, 6),(1, 'grosjean', 19, 6),(1, 'grosjean', 20, 6),(1, 'grosjean', 21, 6),(1, 'grosjean', 22, 6),(1, 'grosjean', 23,6),(1, 'grosjean', 24, 6),(1, 'grosjean', 25, 6),(1, 'grosjean', 26, 6),(1, 'grosjean', 27, 6),(1, 'grosjean', 28, 6),(1, 'grosjean', 29, 6),(1, 'grosjean', 30, 6);
	
INSERT INTO TABLE results
	VALUES
	(1,1,1,'kubica',1,1,2,218.3,25,30,1,6300),
	(2,1,2,'rosberg',4,1,3,217.5,18,30,2,6305),
	(3,1,3,'hamilton',3,1,1,216.7,15,30,3,6310),
	(4,1,4,'vettel',5,1,4,215.4,13,30,4,6313),
	(5,1,5,'max_verstappen',2,1,11,218.3,12,30,5,6317),
	(6,1,6,'grosjean',6,1,29,214.3,11,30,14,6,6320);
	
INSERT INTO TABLE driver_race
	VALUES
	(1,'Robert Kubica','Williams',0,1,'Finished','Polish',91.34,218.3,6300,1,25,30,1),
	(1,'Nico Rosberg','Mercedes',3,2,'Finished','German',91.66,217.3,6305,2,18,30,1),
	(1,'Lewis Hamilton','Mercedes',2,3,'Finished','British',92.77,216.3,6310,3,15,30,1),
	(1,'Sebastian Vettel','Ferrari',1,4,'Finished','German',91.94,215.3,6313,4,13,30,1),
	(1,'Max Verstappen','Renault',2,5,'Finished','Dutch',91.34,218.3,6317,5,12,30,1),
	(1,'Romain Grosjean','Haas',0,6,'Finished','French',92.74,214.3,6320,6,11,30,1);
	
INSERT INTO TABLE race_season
	VALUES
	(1,2019,'monaco',11,0,0);