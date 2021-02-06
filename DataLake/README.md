# Project 4: Data Lake
## Data Engineer: Matthew Prout

## Project Purpose
Sparkify is moving their data from a data warehouse to a data lake.  The purpose of this project is to perform the data engineering tasks to load the data from S3 into Spark, transform the data, and save the dimensional tables back to S3. From there it can be used by the analytics team.

## Raw Data
The song and log data are both JSON files stored in the following buckets on S3:

| Data | Bucket | Partitioning | Example |
| - | - |- | - |
| Log Data | `s3://udacity-dend/log_data` | By year/month | data/log_data/2018/11 |
| Song Data | `s3://udacity-dend/song_data` | By first three letters of song | data/song_data/A/A/A |

## Running the code
At a command prompt run the following command:
`python3 etl.py`

## Design
The code is one python script called etl.py which processes the song and event log data to create the data tables for analysis.  It is broken into two functions: process_song_data() and process_log_data():

**process_song_data()**
This function loads the song data files from S3. It then saves the song information to a parquet file partitioned by year and artist_id.  It also extracts the distinct artists and saves the artist information to a parquet file.

**process_log_data()**
This function loads the event log data (the songs played by the user), and first filters and cleans the data.  
It extracts the unique users and writes them to a parquet file.  A time dimension table is created to provide a fast lookup of date/time fields for joins.  A user defined function is created to convert the ts field from the data to a timestamp, and built-in Spark functions are userd to extract the other fields.  This table is then written to a parquet file partitioned by year and month.
The songplay table is created by joining the event log data with the artist and song tables.  Before it is written to a parquet file, the results from the prior query are joined with the time table to get the year and month of the events, so that the table can be partitioned on year and month.

## Summary of the Files:
| File | Purpose |
| - | - |
| `dl.cfg` | The configuration file where you set your AWS access key ID and secret access key |
| `etl.py` | The main ETL file that processes the song and log data from S3 and saves the new tables back to S3 for the analytics team |
| `README.md` | This README file |
