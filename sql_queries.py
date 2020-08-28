import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        first_name          VARCHAR,
        gender              VARCHAR,
        session_item        INTEGER,
        last_name           VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        BIGINT,
        session_id          INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  BIGINT,
        user_agent          VARCHAR,
        user_id             INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INT         IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time          TIMESTAMP                 NOT NULL DISTKEY SORTKEY,
        user_id             INTEGER                   NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR                   NOT NULL,
        artist_id           VARCHAR                   NOT NULL,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id             INT          NOT NULL PRIMARY KEY SORTKEY,
        first_name          VARCHAR      NOT NULL,
        last_name           VARCHAR      NOT NULL,
        gender              VARCHAR      NOT NULL,
        level               VARCHAR
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR      NOT NULL PRIMARY KEY,
        title               VARCHAR      NOT NULL,
        artist_id           VARCHAR      NOT NULL,
        year                INTEGER      NOT NULL SORTKEY,
        duration            FLOAT
    ) DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id           VARCHAR      NOT NULL PRIMARY KEY,
        name                VARCHAR      NOT NULL,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
     CREATE TABLE IF NOT EXISTS time(
        start_time          TIMESTAMP    NOT NULL PRIMARY KEY DISTKEY SORTKEY,
        hour                INTEGER      NOT NULL,
        day                 INTEGER      NOT NULL,
        week                INTEGER      NOT NULL,
        month               INTEGER      NOT NULL,
        year                INTEGER      NOT NULL,
        weekday             VARCHAR(20)  NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ROLE_ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ROLE_ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second ',
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.session_id,
            e.location,
            e.user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name);
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(user_id),
            first_name,
            last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL;
""")


song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id),
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id),
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second ') AS ts_timestamp,
           EXTRACT(HOUR FROM ts_timestamp),
           EXTRACT(DAY FROM ts_timestamp),
           EXTRACT(WEEK FROM ts_timestamp),
           EXTRACT(MONTH FROM ts_timestamp),
           EXTRACT(YEAR FROM ts_timestamp),
           EXTRACT(DOW FROM ts_timestamp)
    FROM staging_events;
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_rows_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_rows_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_rows_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_rows_users = ("""
    SELECT COUNT(*) FROM users
""")

get_rows_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_rows_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_rows_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
get_rows_table_queries = [get_rows_staging_events, get_rows_staging_songs, get_rows_songplays, get_rows_users, get_rows_songs, get_rows_artists, get_rows_time]
