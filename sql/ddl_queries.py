""" Python file to store queries
"""


import configparser


# DROP THE TABLES TO CLEAN UP DB

staging_events_table_drop = " drop table if exists staging.stg_events"
staging_songs_table_drop = "  drop table if exists staging.stg_songs_data"
songplay_table_drop = " drop table if exists dwh.fact_songplays  "
user_table_drop = " drop table if exists dwh.dim_users "
song_table_drop = " drop table if exists dwh.dim_songs "
artist_table_drop = " drop table if exists dwh.dim_artists "
time_table_drop = " drop table if exists dwh.dim_time "

# CREATE SCHEMA FOR REDSHIFT DATABASE
datawarehouse_schema_create =" create schema if not exists dwh; "
staging_schema_create = "create schema if not exists staging; "

# CREATE TABLES

#Staging table to load event data
#Schema matches the data files
staging_events_table_create= ("""
create table if not exists staging.stg_events
(
  artist varchar(200),
  auth varchar(50),
  firstName varchar(50),
  gender varchar(50),
  itemInSession int,
  lastName varchar(50),
  length float,
  level varchar(50),
  location varchar(100),
  method varchar(10),
  page varchar(50),
  registration varchar(100),
  sessionId bigint ,
  song varchar(200),
  status int,
  ts bigint,
  userAgent varchar(500),
  userId  bigint 
);

""")

#Staging table to load songs data
#Schema matches the data files

staging_songs_table_create = ("""

create table staging.stg_songs_data
(
  song_id VARCHAR(200) ,
  num_songs int,
  title  VARCHAR(200) ,
  artist_name VARCHAR(1000),
  artist_latitude  decimal(11,8),  
  year int,
  duration real,
  artist_id varchar(200) ,
  artist_longitude  decimal(11,8),
  artist_location varchar(4000) 
);



""")

# Distribution Style is All as it will help in joins for analysis to avoid shuffling
# Sort Key: level (assuming data might be grouped by level)
user_table_create = ("""
create table if not exists dwh.dim_users
(
user_id bigint not null primary key ,
first_name varchar,
last_name varchar,
gender varchar,
level varchar(50) sortkey
)
diststyle all;

""")

# Distribution Style is by key(song_id) :As the most likely the join will be with songs dimension table. This will avoid shuffling
# Sort Key : start_time
songplay_table_create = ("""

create table if not exists dwh.fact_songplays 
(
  songplay_id bigint IDENTITY(0,1) not null primary key,
  start_time timestamp not null sortkey,
  user_id  bigint not null ,
  level varchar(50),
  song_id varchar(50) distkey,
  artist_id varchar(50),
  session_id bigint not null,
  location varchar(100),
  user_agent varchar(500),
  length real ,
  item_in_session int,
  registration varchar(100),
  status int,
  auth varchar(50)
  ) diststyle key;
  
""")


# Distribution Style is by key: (song_id) as the most likely the join will be with facts. This will avoid shuffling 
# Sortkey: Year ; as its likely to be used in query
song_table_create = ("""

create table if not exists dwh.dim_songs
(
song_id varchar(50) not null primary key distkey,
title varchar,
artist_id varchar(50),
year int sortkey,
duration real
) diststyle key;

""")

# Distribution Style is All: as it will help in joins for analysis to avoid shuffling
# Sort key: kept artist_id,  as not aware how the table will be queried.
artist_table_create = ("""

create table if not exists dwh.dim_artists
(
artist_id varchar(50) not null primary key sortkey,
name varchar,
location varchar,
latitude  decimal(11,8),
longitude  decimal(11,8)

) diststyle all;

""")

# Distribution Style is All as it will help in joins for analysis to avoid shuffling
# Sort Key : start_time; as most likely will be used for query
time_table_create = ("""

create table if not exists dwh.dim_time
(
start_time  timestamp  not null primary key sortkey,
hour integer,
day integer, 
week integer, 
month integer,
year integer,
weekday integer
)
diststyle all;
""")


ddl_schema_queries = [staging_schema_create, datawarehouse_schema_create]
ddl_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_tables = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
