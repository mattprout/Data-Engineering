# DROP TABLES

staging_visit_table_drop = "DROP TABLE IF EXISTS staging_visit;"
staging_demographics_table_drop = "DROP TABLE IF EXISTS staging_demographics;"
staging_oilprice_table_drop = "DROP TABLE IF EXISTS staging_oilprice;"
staging_tempbystate_table_drop = "DROP TABLE IF EXISTS staging_tempbystate;"
staging_datetime_table_drop = "DROP TABLE IF EXISTS staging_datetime;"

visit_table_drop = "DROP TABLE IF EXISTS visit;"
demographics_table_drop = "DROP TABLE IF EXISTS demographics;"
oilprice_table_drop = "DROP TABLE IF EXISTS oilprice;"
tempbystate_table_drop = "DROP TABLE IF EXISTS tempbystate;"
datetime_table_drop = "DROP TABLE IF EXISTS datetime;"

# S3 Keys

visit_key = 'i94'
demographics_key = 'demographics'
oilprice_key = 'oilprices'
tempbystate_key = 'temperatures'

# COPY COMMAND

staging_copy = """
COPY {}
FROM '{}'
IAM_ROLE '{}'
FORMAT AS PARQUET;"""


# CREATE TABLES

staging_visit_table_create= ("""
CREATE TABLE staging_visit(
	id INT NOT NULL PRIMARY KEY,
	citizen NVARCHAR(50),
	resident NVARCHAR(50),
	port_name NVARCHAR(50),
	port_state NCHAR(2),
	mode INT,
	destination_state NCHAR(2),
	visa INT NOT NULL,
	arrival_date DATE,
	departure_date DATE
);""")

staging_demographics_table_create= ("""
CREATE TABLE staging_demographics(
  	city NVARCHAR(50) NOT NULL,
  	state NCHAR(2) NOT NULL,
  	median_age FLOAT NOT NULL,
  	total_population INT NOT NULL,
  	foreign_born_pct FLOAT NOT NULL,
	year INT NOT NULL
);""")

staging_oilprice_table_create= ("""
CREATE TABLE staging_oilprice(
	year INT NOT NULL,
	month INT NOT NULL,
	monthly_average_price FLOAT NOT NULL
);""")

staging_tempbystate_table_create= ("""
CREATE TABLE staging_tempbystate(
	year INT NOT NULL,
	month INT NOT NULL,
	country NVARCHAR(50) NOT NULL,
	state NVARCHAR(50) NOT NULL,
	monthly_average_temp FLOAT NOT NULL
);""")

staging_datetime_table_create= ("""
CREATE TABLE staging_datetime(
	event DATE NOT NULL PRIMARY KEY,
	year INT NOT NULL,
	month INT NOT NULL,
	day INT NOT NULL,
	weekday BOOLEAN NOT NULL,
	weekend BOOLEAN NOT NULL
);""")


visit_table_create= ("""
CREATE TABLE visit(
	id INT NOT NULL PRIMARY KEY,
	citizen NVARCHAR(50),
	resident NVARCHAR(50),
	port_name NVARCHAR(50),
	port_state NCHAR(2),
	mode INT,
	destination_state NCHAR(2),
	visa INT NOT NULL,
	arrival_date DATE SORTKEY,
	departure_date DATE
);""")

demographics_table_create= ("""
CREATE TABLE demographics(
  	city NVARCHAR(50) NOT NULL,
  	state NCHAR(2) NOT NULL,
  	median_age FLOAT NOT NULL,
  	total_population INT NOT NULL,
  	foreign_born_pct FLOAT NOT NULL,
	year INT NOT NULL
) DISTSTYLE ALL;""")

oilprice_table_create= ("""
CREATE TABLE oilprice(
	year INT NOT NULL,
	month INT NOT NULL,
	monthly_average_price FLOAT NOT NULL
) DISTSTYLE ALL;""")

tempbystate_table_create= ("""
CREATE TABLE tempbystate(
	year INT NOT NULL,
	month INT NOT NULL,
	country NVARCHAR(50) NOT NULL,
	state NVARCHAR(50) NOT NULL,
	monthly_average_temp FLOAT NOT NULL
) DISTSTYLE AUTO;""")

datetime_table_create= ("""
CREATE TABLE datetime(
	event DATE NOT NULL PRIMARY KEY,
	year INT NOT NULL,
	month INT NOT NULL,
	day INT NOT NULL,
	weekday BOOLEAN NOT NULL,
	weekend BOOLEAN NOT NULL
) DISTSTYLE AUTO;""")


# TRUNCATE STAGING TABLES

visit_table_truncate = ("""
TRUNCATE TABLE staging_visit;
""")

demographics_table_truncate = ("""
TRUNCATE TABLE staging_demographics;
""")

datetime_table_truncate = ("""
TRUNCATE TABLE staging_datetime;
""")

oilprices_table_truncate = ("""
TRUNCATE TABLE staging_oilprice;
""")

tempbystate_table_truncate = ("""
TRUNCATE TABLE staging_tempbystate;
""")


# DELETE DUPLICATES FOR UPDATE

delete_table_visit= ("""
DELETE FROM staging_visit
USING visit
WHERE staging_visit.id = visit.id;
""")

delete_table_demographics= ("""
DELETE FROM staging_demographics
USING demographics
WHERE (staging_demographics.year = demographics.year) AND
      (staging_demographics.state = demographics.state) AND
      (staging_demographics.city = demographics.city);
""")

delete_table_oilprices= ("""
DELETE FROM staging_oilprice
USING oilprice
WHERE (staging_oilprice.year = oilprice.year) AND
      (staging_oilprice.month = oilprice.month);
""")

delete_table_tempbystate= ("""
DELETE FROM staging_tempbystate
USING tempbystate
WHERE (staging_tempbystate.country = tempbystate.country) AND
      (staging_tempbystate.state = tempbystate.state) AND
      (staging_tempbystate.year = tempbystate.year) AND
      (staging_tempbystate.month = tempbystate.month);
""")


# INSERT TO STAGING TABLES

visit_table_insert= ("""
INSERT INTO visit
SELECT * FROM staging_visit;""")

demographics_table_insert= ("""
INSERT INTO demographics
SELECT * FROM staging_demographics;""")

oilprices_table_insert= ("""
INSERT INTO oilprice
SELECT * FROM staging_oilprice;""")

tempbystate_table_insert= ("""
INSERT INTO tempbystate
SELECT * FROM staging_tempbystate;""")

datetime_table_insert = ("""
INSERT INTO datetime
SELECT A.event,
EXTRACT(YEAR FROM A.event),
EXTRACT(MONTH FROM A.event),
EXTRACT(DAY FROM A.event),
EXTRACT(DOW FROM A.event) > 0 AND EXTRACT(DOW FROM A.event) < 6,
EXTRACT(DOW FROM A.event) = 0 OR EXTRACT(DOW FROM A.event) = 6
FROM
(SELECT arrival_date AS event FROM staging_visit
UNION
SELECT departure_date AS event FROM staging_visit) A
WHERE (A.event IS NOT NULL) AND (A.event NOT IN (SELECT datetime.event FROM datetime));""")

# QUERY LISTS

create_table_queries = [staging_visit_table_create, staging_demographics_table_create, staging_oilprice_table_create, staging_tempbystate_table_create, staging_datetime_table_create, visit_table_create, demographics_table_create, oilprice_table_create, tempbystate_table_create, datetime_table_create]
drop_table_queries = [staging_visit_table_drop, staging_demographics_table_drop, staging_oilprice_table_drop, staging_tempbystate_table_drop, staging_datetime_table_drop, visit_table_drop, demographics_table_drop, oilprice_table_drop, tempbystate_table_drop, datetime_table_drop]
truncate_table_queries = [visit_table_truncate, demographics_table_truncate, oilprices_table_truncate, tempbystate_table_truncate, datetime_table_truncate]
delete_duplicate_queries = [delete_table_visit, delete_table_demographics, delete_table_oilprices, delete_table_tempbystate]
insert_table_queries = [visit_table_insert, demographics_table_insert, oilprices_table_insert, tempbystate_table_insert, datetime_table_insert]
