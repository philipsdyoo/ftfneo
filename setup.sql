-- neo stores the NEO data
DROP TABLE IF EXISTS neo;
CREATE TABLE IF NOT EXISTS neo (
	id INT NOT NULL AUTO_INCREMENT,
	neo_reference_id VARCHAR(4) NOT NULL,
	name VARCHAR(100) NOT NULL,
	nasa_jpl_url VARCHAR(100) NOT NULL,
	absolute_magnitude_h DECIMAL(10, 2) NOT NULL,
	is_potentially_hazardous_asteroid BOOLEAN NOT NULL,
	is_sentry_object BOOLEAN NOT NULL,
	estimated_diameter_min DECIMAL(20, 10) NOT NULL,
	estimated_diameter_max DECIMAL(20, 10) NOT NULL,
	close_approach_datetime DATETIME NOT NULL,
	relative_velocity DECIMAL(30, 10) NOT NULL,
	miss_distance DECIMAL(30, 10) NOT NULL,
	orbiting_body VARCHAR(100) NOT NULL,
	PRIMARY KEY(id)
);
-- neo_load stores one row which holds details about the data loading process
DROP TABLE IF EXISTS neo_load;
CREATE TABLE IF NOT EXISTS neo_load (
  id INT NOT NULL AUTO_INCREMENT,
  running BOOLEAN NOT NULL,
  start_dt DATETIME NOT NULL,
  end_dt DATETIME NOT NULL,
  PRIMARY KEY(id)
);
INSERT INTO neo_load (running, start_dt, end_dt) VALUES (FALSE, NOW(), NOW());
