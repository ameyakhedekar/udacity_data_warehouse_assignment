### Introduction

The purpose of this reporting schema is to do report and analyze the data on Songs, artists , users and playlist of the songs.



### Database:

#### Staging Schema
Schema to load data from external sources
Schema Name: staging
Tables:
1. staging.stg_songs_data: Staging table for songs data
2. staging.stg_events: Staging table for events data

#### Reporting Schema

The Reporting schema is a star schema with facts table for metrics and dimensions table. 
Schema Name: dwh

#### Fact tables:
1. dwh.fact_songplays : The fact table which contain the records for every song play event. 

#### Dimension tables:
1. dwh.dim_songs : Dimension table for list of songs. Key to join with facts - song_id
2. dwh.dim_artists : Dimension table for list of artists. Key to join with facts - artist_id
3. dwh.dim_time : Dimension table for time series/aggregation reporting. Key to join with fact - start_time
4. dwh.dim_users  : Dimenstion table for users. Key to join with fact - userid

#### Sample query
To get most popular artists:

```
select a.name, count(*) from dwh.dim_artists a inner join dwh.fact_songplays sp on a.artist_id = sp.artist_id group by a.name order by count desc
```

### Project files and Execution

#### How to run the project:

Prerequisites:
1. Upload the _song_data_jsonpath.json_ file in an s3 bucket
2. Update config file in _config/dwh.cfg_  appropriately.  The Secret key, access key should have admin access to the account. SONG_JSONPATH value should be the location of the _song_data_jsonpath.json_ file in S3.
2. The default VPN/Vpn used by the redshift server should have inbount traffic open for redshift port(5439).

To Run the job:
1. Go in the project folder _Data_Warehouse_Project_.
2. Run _etl.py_ file.
What to expect:
```
Role dwhEtlRole created
Redshift Cluster sparkify-dwh created
Infrastructure Created. Please wait till the cluster is ready
Waiting for cluster to be ready...(Checking in 20 secs)
Waiting for cluster to be ready...(Checking in 20 secs)
.
.
Running Steps:
Start LogDataStagingLoadEtlStep
Uploaded table: staging.stg_events from S3 Path: s3://udacity-dend/log_data
 End LogDataStagingLoadEtlStep
Start SongStagingLoadEtlStep
Uploaded table: staging.stg_songs_data from S3 Path: s3://udacity-dend/song_data
End SongStagingLoadEtlStep
start CreateArtistDimensionsEtlStep
end CreateArtistDimensionsEtlStep
start CreateDateDimensionsEtlStep
end CreateDateDimensionsEtlStep
start CreateSongDimensionsEtlStep
start CreateSongDimensionsEtlStep
start CreateUserDimensionsEtlStep
end CreateUserDimensionsEtlStep
start CreateSongPlayFactsEtlStep
End CreateSongPlayFactsEtlStep
Job Completed.

```
3. Once the job is run, you can connect to the redshift cluster and query the data. *Check Sample query above*

To tear down the cluster:
1. Go in the project folder _Data_Warehouse_Project_.
2. Run _tear_down_infra.py_


#### Files:
The important files, folders are:
1. config: Contains config artifcats required by the app to run.
2. infrastructure: Classes to manage infrastructure.
3. sql: Classes and artifiacts to manage Database definition.
4. utils: helper classes used in the code.
5. steps: Classes for ETL steps.
6. etl.py: The etl job to execute
7. _tear_down_infra.py_ : The script to tear down the cluster


### Notes
Assumptions made/Further discussions if required
1. instead of updating the config file, I created a wrapper class for infrastructuresetting, which will return the cluster information. 
2. The given data set has artists and songs, which do not match with the dimensions. Choose not to remove them from facts table. 
3. Tried to keep class names, and function name verbose, so that the code reader can read the code easily.
Any feedback is welcome.


