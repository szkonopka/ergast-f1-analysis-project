set hive.execution.engine=mr;
INSERT INTO TABLE status
	VALUES (1, 'Finished'), (2, 'Disqualified'), (3, 'Accident'), (4, 'Collision'), (5, 'Engine'), (6, 'Gearbox'), (7, 'Transmission'), (8, 'Clutch'), (9, 'Hydraulics'), (10, 'Electrical'), (11, 'Power Unit'), (12, 'Collision damage'), (13, 'Brake duct'), (14, 'Seat'), (15, 'Damage');

INSERT INTO TABLE circuits
	VALUES ('monaco', 'Circuit de Monaco'), ('hockenheimring', 'Hockenheimring'), ('bahrain', 'Bahrain International Circuit'), ('monza', 'Autodromo Nazionale di Monza'), ('suzuka','Suzuka Circuit'), ('sochi','Sochi Autodrom'), ('red_bull_ring','Red Bull Ring'), ('hungaroring','Hungaroring'), ('silverstone','Silverstone Circuit'), ('albert_park','Albert Park Grand Prix Circuit');
	
INSERT INTO TABLE constructors
	VALUES ('mclaren', 'McLaren', 'British'), ('williams','Williams','British'), ('renault','Renault','French'), ('ferrari','Ferrari','Italian'), ('toro_rosso','Toro Rosso','Italian'), ('force_india','Force India','Indian'), ('alfa','Alfa Romeo','Italian'), ('haas','Haas F1 Team','American'), ('racing_point','Racing Point','British'), ('mercedes','Mercedes','German');
	
INSERT INTO TABLE races
	VALUES (1, 'monaco', 2019), (2,'hockenheimring', 2019), (3,'bahrain',2019), (4,'monza',2019), (5,'suzuka',2019), (6,'sochi',2019), (7,'red_bull_ring',2019), (8,'hungaroring',2019), (9,'silverstone',2019), (10,'albert_park',2019);