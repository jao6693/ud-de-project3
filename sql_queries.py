import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS f_songplays;"
user_table_drop = "DROP TABLE IF EXISTS d_users;"
song_table_drop = "DROP TABLE IF EXISTS d_songs;"
artist_table_drop = "DROP TABLE IF EXISTS d_artists;"
time_table_drop = "DROP TABLE IF EXISTS d_times;"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS stg_events ( \
  artist varchar(200), \
  auth varchar(10) NOT NULL, \
  firstName varchar(80) NOT NULL, \
  gender character NOT NULL, \
  itemInSession integer, \
  lastName varchar(100) NOT NULL, \
  length numeric, \
  level varchar(10) NOT NULL, \
  location varchar(200) NOT NULL, \
  method varchar(4) NOT NULL, \
  page varchar(10) NOT NULL, \
  registration numeric NOT NULL, \
  sessionId int, \
  song varchar(200), \
  status int NOT NULL, \
  ts bigint NOT NULL, \
  userAgent varchar(200) NOT NULL, \
  userId int NOT NULL, \
  PRIMARY KEY (sessionId, itemInSession) \
);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS stg_songs ( \
  num_songs int NOT NULL, \
  artist_id varchar(18) NOT NULL, \
  latitude numeric, \
  longitude numeric, \
  location varchar(200), \
  name varchar(200) NOT NULL, \
  song_id varchar(18), \
  title varchar(200) NOT NULL, \
  duration numeric NOT NULL, \
  year smallint, \
  PRIMARY KEY(song_id) \
);")

"""
songplay_table_create = ("CREATE TABLE IF NOT EXISTS f_songplays ( \
  songplay_id IDENTITY(0, 1) PRIMARY KEY, \
  start_time timestamp REFERENCES d_times(start_time), \
  user_id int REFERENCES d_users(user_id), \
  level varchar(10), \
  song_id varchar(18) REFERENCES d_songs(song_id), \
  artist_id varchar(18) REFERENCES d_artists(artist_id), \
  session_id int, \
  location varchar(200), \
  user_agent varchar(200) \
);")
"""
songplay_table_create = ("CREATE TABLE IF NOT EXISTS f_songplays ( \
  songplay_id SERIAL PRIMARY KEY, \
  start_time timestamp REFERENCES d_times(start_time), \
  user_id int REFERENCES d_users(user_id), \
  level varchar(10), \
  song_id varchar(18) REFERENCES d_songs(song_id), \
  artist_id varchar(18) REFERENCES d_artists(artist_id), \
  session_id int, \
  location varchar(200), \
  user_agent varchar(200) \
);")

user_table_create = ("CREATE TABLE IF NOT EXISTS d_users ( \
  user_id int PRIMARY KEY, \
  first_name varchar(80) NOT NULL, \
  last_name varchar(100) NOT NULL, \
  gender character NOT NULL, \
  level varchar(10) NOT NULL \
);")

song_table_create = ("CREATE TABLE IF NOT EXISTS d_songs ( \
  song_id varchar(18) PRIMARY KEY, \
  title varchar(200) NOT NULL, \
  artist_id varchar(18) NOT NULL, \
  year int, \
  duration numeric \
);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS d_artists ( \
  artist_id varchar(18) PRIMARY KEY, \
  name varchar(200) NOT NULL, \
  location varchar(200), \
  latitude numeric, \
  longitude numeric \
);")

time_table_create = ("CREATE TABLE IF NOT EXISTS d_times ( \
  start_time timestamp PRIMARY KEY, \
  hour smallint, \
  day smallint NOT NULL, \
  week smallint NOT NULL, \
  month smallint NOT NULL, \
  year smallint NOT NULL, \
  weekday smallint NOT NULL \
);")

# STAGING TABLES

staging_events_copy = ("""
  copy {} from {}
  credentials 'aws_iam_role={}'
  gzip region 'us-west-2';
""").format('stg_events', config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
  copy {} from {}
  credentials 'aws_iam_role={}'
  gzip region 'us-west-2';
""").format('stg_songs', config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("INSERT INTO f_songplays \
  (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
  SELECT to_timestamp(events.ts/1000) AT TIME ZONE 'UTC' AS start_time, \
    events.userId AS user_id, \
    songs.song_id AS song_id, \
    songs.artist_id AS artist_id, \
    events.sessionId AS session_id, \
    events.location AS location, \
    events.userAgent AS user_agent \
  FROM stg_events AS events \
  JOIN stg_songs AS songs \
  ON songs.artist_name = events.artist \
  AND songs.title = events.song \
);")

user_table_insert = ("INSERT INTO d_users \
  (user_id, first_name, last_name, gender, level) \
  SELECT DISTINCT ON (userId) AS user_id, \
    firstName AS first_name,  \
    lastName AS last_name, \
    gender, \
    level \
  FROM stg_events \
  ON CONFLICT (user_id) DO UPDATE \
  SET level = EXCLUDED.level;")

song_table_insert = ("INSERT INTO d_songs \
  (song_id, title, artist_id, year, duration) \
  SELECT song_id, \
    artist_id, \
    year, \
    duration \
  FROM stg_songs \
  ON CONFLICT DO NOTHING;")

artist_table_insert = ("INSERT INTO d_artists \
  (artist_id, name, location, latitude, longitude) \
  SELECT artist_id, \
    name, \
    location, \
    latitude, \
    longitude \
  FROM stg_songs \
  ON CONFLICT DO NOTHING;")

time_table_insert = ("INSERT INTO d_times \
  (start_time, hour, day, week, month, year, weekday) \
  SELECT to_timestamp(ts/1000) AT TIME ZONE 'UTC' AS start_time, \
    EXTRACT(hour FROM to_timestamp(ts/1000) AT TIME ZONE 'UTC') AS hour, \
    EXTRACT(day FROM to_timestamp(ts/1000) AT TIME ZONE 'UTC') AS day, \
    EXTRACT(week FROM to_timestamp(ts/1000) AT TIME ZONE 'UTC') AS week, \
    EXTRACT(month FROM to_timestamp(ts/1000) AT TIME ZONE 'UTC') AS month, \
    EXTRACT(year FROM to_timestamp(ts/1000) AT TIME ZONE 'UTC') AS year, \
    EXTRACT(DOW FROM to_timestamp(ts/1000) AT TIME ZONE 'UTC') AS weekday \
  FROM stg_events \
  ON CONFLICT DO NOTHING;")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert, songplay_table_insert]
