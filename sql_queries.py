import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
REGION = config.get("AWS", "REGION")
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

DROP_TABLE = "DROP TABLE IF EXISTS "

staging_events_table_drop = DROP_TABLE + "staging_events;"
staging_songs_table_drop = DROP_TABLE + "staging_songs;"
songplay_table_drop = DROP_TABLE + "songplays;"
user_table_drop = DROP_TABLE + "users;"
song_table_drop = DROP_TABLE + "songs;"
artist_table_drop = DROP_TABLE + "artists;"
time_table_drop = DROP_TABLE + "time;"

# CREATE TABLES

staging_events_table_create = "CREATE TABLE IF NOT EXISTS " + "staging_events ("
staging_events_table_create += "artist VARCHAR, "
staging_events_table_create += "auth VARCHAR, "
staging_events_table_create += "firstName VARCHAR, "
staging_events_table_create += "gender VARCHAR, "
staging_events_table_create += "itemInSession INTEGER, "
staging_events_table_create += "lastName VARCHAR, "
staging_events_table_create += "length FLOAT, "
staging_events_table_create += "level VARCHAR, "
staging_events_table_create += "location VARCHAR, "
staging_events_table_create += "method VARCHAR, "
staging_events_table_create += "page VARCHAR, "
staging_events_table_create += "registration FLOAT, "
staging_events_table_create += "sessionId INTEGER, "
staging_events_table_create += "song VARCHAR, "
staging_events_table_create += "status INTEGER, "
staging_events_table_create += "ts TIMESTAMP, "
staging_events_table_create += "userAgent VARCHAR, "
staging_events_table_create += "userId INTEGER );"

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS " + "staging_songs ("
staging_songs_table_create += "num_songs INTEGER, "
staging_songs_table_create += "artist_id VARCHAR, "
staging_songs_table_create += "artist_latitude FLOAT, "
staging_songs_table_create += "artist_longitude FLOAT, "
staging_songs_table_create += "artist_location VARCHAR, "
staging_songs_table_create += "artist_name VARCHAR, "
staging_songs_table_create += "song_id VARCHAR, "
staging_songs_table_create += "title VARCHAR, "
staging_songs_table_create += "duration FLOAT, "
staging_songs_table_create += "year INTEGER );"


songplay_table_create = "CREATE TABLE IF NOT EXISTS " + "songplays ("
songplay_table_create += "songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, "
songplay_table_create += "start_time TIMESTAMP NOT NULL SORTKEY DISTKEY, "
songplay_table_create += "user_id INTEGER NOT NULL, "
songplay_table_create += "level VARCHAR, "
songplay_table_create += "song_id VARCHAR NOT NULL, "
songplay_table_create += "artist_id VARCHAR NOT NULL, "
songplay_table_create += "session_id INTEGER NOT NULL, "
songplay_table_create += "location VARCHAR, "
songplay_table_create += "user_agent VARCHAR);"

user_table_create = "CREATE TABLE IF NOT EXISTS " + "users ("
user_table_create += "user_id INTEGER PRIMARY KEY SORTKEY, "
user_table_create += "first_name VARCHAR NOT NULL, "
user_table_create += "last_name VARCHAR, "
user_table_create += "gender VARCHAR, "
user_table_create += "level VARCHAR);"

song_table_create = "CREATE TABLE IF NOT EXISTS " + "songs ("
song_table_create += "song_id VARCHAR PRIMARY KEY SORTKEY, "
song_table_create += "title VARCHAR NOT NULL, "
song_table_create += "artist_id VARCHAR NOT NULL, "
song_table_create += "year INTEGER, "
song_table_create += "duration DECIMAL );"

artist_table_create = "CREATE TABLE IF NOT EXISTS " + "artists ("
artist_table_create += "artist_id VARCHAR PRIMARY KEY SORTKEY, "
artist_table_create += "name VARCHAR NOT NULL, "
artist_table_create += "location VARCHAR, "
artist_table_create += "latitude VARCHAR, "
artist_table_create += "longitude VARCHAR );"

time_table_create = "CREATE TABLE IF NOT EXISTS " + "time ("
time_table_create += "start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY, "
time_table_create += "hour INTEGER NOT NULL, "
time_table_create += "day INTEGER NOT NULL, "
time_table_create += "week INTEGER NOT NULL, "
time_table_create += "month INTEGER NOT NULL, "
time_table_create += "year INTEGER NOT NULL, "
time_table_create += "weekday INTEGER NOT NULL );"

# STAGING TABLES

staging_events_copy = f"COPY staging_events FROM {LOG_DATA} "
staging_events_copy += f"CREDENTIALS 'aws_iam_role={ARN}' "
staging_events_copy += f"REGION '{REGION}' FORMAT AS JSON {LOG_JSONPATH} "
staging_events_copy += "timeformat as 'epochmillisecs';"

staging_songs_copy = f"COPY staging_songs FROM {SONG_DATA} "
staging_songs_copy += f"CREDENTIALS 'aws_iam_role={ARN}' "
staging_songs_copy += f"REGION '{REGION}' FORMAT AS JSON 'auto' truncatecolumns;"


# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  e.ts start_time, e.userId user_id, e.level, s.song_id, s.artist_id, 
            e.sessionId session_id, e.location, e.userAgent user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title)
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId user_id, firstName first_name, lastName last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name name, artist_location AS location,
            artist_latitude AS latitude, artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(DAYOFWEEK FROM start_time) AS weekday
    FROM songplays;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
