import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS logs_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS logs_stage (
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstname VARCHAR,
    gender CHAR(1),
    iteminsession SMALLINT NOT NULL,
    lastname VARCHAR,
    length DECIMAL(10,5),
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration DECIMAL,
    sessionid INT NOT NULL,
    song VARCHAR,
    status SMALLINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    useragent VARCHAR,
    userid INT
);""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_stage (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    duration DECIMAL(10,5),
    year SMALLINT,
    artist_name VARCHAR NOT NULL,
    artist_latitude DECIMAL(6,3),
    artist_longitude DECIMAL(6,3),
    artist_location VARCHAR
);""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY (0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
);""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR NOT NULL
) DISTSTYLE AUTO;""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year SMALLINT,
    duration DECIMAL(10,5)
) DISTSTYLE ALL;""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DECIMAL(6,3),
    longitude DECIMAL(6,3)
) DISTSTYLE ALL;""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
);""")

# STAGING TABLES

staging_events_copy = """COPY logs_stage
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF
REGION 'us-west-2'
JSON {}
TIMEFORMAT 'epochmillisecs';""".format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'));

staging_songs_copy = """COPY songs_stage
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF
REGION 'us-west-2'
JSON 'auto ignorecase';""".format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    LS.ts AS start_time,
    LS.userid AS user_id,
    LS.level,
    (SELECT songs.song_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE (songs.title = LS.song) AND (artists.name = LS.artist) AND (songs.duration = LS.length)) AS song_id,
    (SELECT artists.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE (songs.title = LS.song) AND (artists.name = LS.artist) AND (songs.duration = LS.length)) AS artist_id,
    LS.sessionId AS session_id,
    LS.location,
    LS.useragent AS user_agent
FROM logs_stage LS
WHERE (LS.page = 'NextSong');""")

#
# NOTE: Special handling of the user table
#
#user_table_insert = ("""
#""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM songs_stage
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT artist_id,
    artist_name as name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM songs_stage
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);""")

time_table_insert = ("""
INSERT INTO time
SELECT
    DISTINCT ts AS start_time,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day,
    EXTRACT(week FROM ts) AS week,
    EXTRACT(month FROM ts) AS month,
    EXTRACT(year FROM ts) AS year,
    EXTRACT(dow FROM ts) AS weekday
FROM logs_stage
WHERE (page = 'NextSong') AND (ts NOT IN (SELECT DISTINCT start_time FROM time));""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
