# Introduction to Data Warehouses Project 3
# Creating a Data Warehouse for Sparkify
### Data Engineer: Matthew Prout

<br/>

## Project Purpose

The Sparkify company wants to move their ETL process to the cloud.  In order to do this, data will be deployed and infrastructure created on AWS to implement the ETL pipeline.
Sparkify's data consists of user logs (JSON format) and song/artist data from a song database (also in JSON format).
Sparkify will use S3 to store the log and song data, and ETL scripts running on an EC2 instance will create a database on Redshift, copy data to staging table, and then copy the data from the staging tables to tables to the data warehouse.
The purpose of the two step process (using intermediate staging tables) enables the extract, transform, and load steps of the ETL process:
1. Extract: data is copied from a remote location (S3) to a staging table.
2. Transform/Load: data is transformed using SQL statements as it is copied into the tables in the data warehouse.

The star schema is needed for the kinds of querying that the analytics team will perform, which avoids the many (slow) joins needed in third normal form tables, and is efficient for aggregations.


## Raw Data
The song and log data are both JSON files stored in the following buckets on S3:

| Data | Bucket | Partitioning | Example |
| - | - |- | - |
| Log Data | `s3://udacity-dend/log_data` | By year/month | data/log_data/2018/11 |
| Song Data | `s3://udacity-dend/song_data` | By first three letters of song | data/song_data/A/A/A |



## ETL Pipeline

### Copying the Data to Staging Tables

The ETL pipeline uses the Redshift COPY command to copy data from the song and log JSON files directly into the staging tables.  Important considerations to make this work are providing credentials of an IAM role that can access the S3 buckets and specifying the region of the S3 buckets. Because these files are JSON, the 'JSON' attribute needs to be specified along with the column name mapping.  For the song data, I kept the columns of the staging table the same as the JSON data, and for the log data, a JSONPATH file was provided to define the column naming for the JSON file. Finally, for the log data, the TIMEFORMAT property is necessary to translate the timestamp into epoch time from the log file.

### Copying the Data from Staging Tables to the Data Warehouse

Once the data has been successfully copied to the staging tables, it is then transformed and loaded into the data warehouse tables using SQL INSERT statements.  Due to the fact that Redshift does not support 'upsert', special handling is necessary to update the user information (the 'level' field can change based on the user's current subscription).

## Database Design

### Tables

**songs_stage**  
The fields of this table are named after the JSON log file fields.  'song_id' is the primary key from the database where this file came from, and is retained as the primary key in this table.

**logs_stage**  
The fields of this table are named after the JSON log file fields.

**songplays**  
This table is the fact table in the data warehouse.  An identity field is used for the primary key.  The foreign keys to the dimension tables are start_time, user_id, song_id, and artist_id.  
A SORTKEY has been added to the start_time field in order to improve the performance of queries (range, sorting) based on this field.  

**users**  
The users dimension table contains information about the users.  The user_id primary key was defined in the log data, so it is kept as the primary key in this table.  This table requires inserts for new users, along with updates for changes to user's subscription level ('level').  Updating this table is handled as a special case in the code.  
The AUTO distribution style is used for this table.  This is because the table is relatively small now, so it can be distributed to each node in the cluster. However as the subscriber base grows larger, the table may become too large to distribute that way.  In that case, Redshift will change the distribution strategy automatically.

**songs**  
The songs dimension table contains information about the songs listened to by users.  The song_id primary key was defined in the song data files, so it is kept as the primary key in this table.  
The ALL distribution style is used for this table.  This is because the number of songs is not that large (~15K), and is not expected to grow that fast.  Because of that, this table can be distributed to the nodes in the cluster.

**artists**  
The artists dimension table contains information about the artists that wrote the songs listened to by users.  The artist_id primary key was defined in the song data files, so it is kept as the primary key in this table.  
The ALL distribution style is used for this table.  This is because the number of artists is not that large (~10K), and is not expected to grow that fast.  Because of that, this table can be distributed to the nodes in the cluster.

**time**  
The time dimension table is a denormalization of the timestamp field for song that were played.  This table provides faster aggregation based on date/time components (day, month, year, etc.).


### Transform and Load Dimension Tables

The transform and load of the data from the staging tables into the dimension tables is performed primarily by SQL INSERT statements.  Because Redshift does not support 'upsert', care must be taken when merging the new data into the dimension tables to avoid primary key errors.  
  
A common idiom for data that is just being added to is to add a check (subquery) in the WHERE clause of the INSERT statement to ensure that the primary key of the new data does not already exist in the table.  
  
Here are comments for how I deal with duplicate records for different tables:  
* The song data is unique when pulled from the song data files.  Deduplication is not necessary prior to inserting the data into the song table.
* The artist data is not necessarily unique when pulled from the song data files, as artists can write more than one song.  DISTINCT is used prior to inserting the data into the artist table.
* Time data is not necessarily unique, as it comes from multiple users who may start listening at the same time.  DISTINCT is used prior to inserting the data into the time table.  Also because the time data comes from the log_stage table, only events where the user navigates to the 'NextSong' page should be used, so this condition is added to the WHERE clause.

Updating the users table differs from the other dimension table because both new users are added and other data (user subscription 'level') is updated.  Because Redshift does not support 'upsert', this condition needs to be handled specially:
1. The latest user information is extracted from the 'log_stage' table into a temporary table 'temp_users'.  This ensures that the most current value for 'level' is obtained for a user.
2. An UPDATE statement is performed on the users table joined with the temp_users table to update the 'level' field on existing users.
3. An INSERT statement is performed on the users table from the temp_users table, inserting only the new users.


### Transform and Load Fact Table

Once the song and artist data has been updated in the dimension tables, the song play events can be loaded into the 'songplays' fact table from the logs_stage table.  This ordering is done so that the song_id and artist_id foreign keys can be obtained from the most complete data, which is obtained by using correlated subqueries with the song and artist tables (the ground truth).  
Note that joining the logs_stage table with the song_stage table could be problematic because the song_stage table may not have all the song and artist data, since the song data is periodically updated.  
The INSERT selects only the data where the user navigates to the 'NextSong' page.


## Running the code
1. First update `dwh.cfg` with the database connection properties under CLUSTER and IAM_ROLE
2. Create the database tables: `python3 create_tables.py`
3. Run the ETL: `python3 etl.py`

## Summary of the Files:
| File | Purpose |
| - | - |
| `create_tables.py` | Methods to recreate the tables in the database |
| `dwh.cfg` | Contains database connection properties |
| `etl.py` | Loads data into the staging tables, and then into the data warehouse |
| `README.md` | This README file |
| `sql_queries.py` | Defines the SQL statements to create, drop, copy, and insert data into the database |
