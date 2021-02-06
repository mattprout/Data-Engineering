import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Lng
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    process_song_data - Loads the song data files from S3, and saves the song information to a parquet file
    (parititioned by year and artist_id), and then extracts the distinct artists and saves them to a parquet file.
    """
    
    # Get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
#    song_data = os.path.join(input_data,'song_data/A/A/A/TRAAAAK128F9318786.json')
    
    songSchema = R([
        Fld("num_songs",Int()),
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_name",Str()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("duration",Dbl()),
        Fld("year",Int())
    ])
    
    # Read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # Extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data,'songs.parquet'))

    # Extract columns to create artists table, and find the distinct artists
    artists_table = df.select(['artist_id',
                               'artist_name',
                               'artist_location',
                               'artist_latitude',
                               'artist_longitude']).withColumnRenamed('artist_name', 'name').withColumnRenamed('artist_location', 'location').withColumnRenamed('artist_latitude', 'latitude').withColumnRenamed('artist_longitude', 'longitude').distinct()
    
    # Write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data,'artists.parquet'))


def process_log_data(spark, input_data, output_data):
    """
    process_log_data - This function loads the event log data (the songs played by the user), and extracts the
    following tables, which are saved to parquet files:
    users: user information (saving it to a parquet file)
    time: the start times and various date/time field extracted from it
    songplays: the event log data with foreign keys to the song and artist tables
    """
    
    # Get filepath to log data file
    log_data = os.path.join(input_data,'log_data/*/*/*.json')
#    log_data = os.path.join(input_data,'log_data/2018/11/2018-11-01-events.json')

    # Read log data file
    df = spark.read.json(log_data)
    
    # Filter by actions for song plays. Remove rows with blank userIds.
    df = df.filter((df.userId != '') & (df.page == 'NextSong'))
    df = df.withColumn('userId', col('userId').cast(Lng()))

    # Extract columns for users table
    df.createOrReplaceTempView("event_log")
    
    # The select statement finds the latest user information, so that the 'level' field is most up-to-date.
    users_table = spark.sql('''
        SELECT
            EL1.userid AS user_id,
            EL1.firstname AS first_name,
            EL1.lastname AS last_name,
            EL1.gender,
            EL1.level
        FROM event_log EL1
        WHERE EL1.ts = (SELECT MAX(EL2.ts) FROM event_log EL2 WHERE EL1.userid = EL2.userid)''')
    
    # Write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data,'users.parquet'))

    # Create a user defined function to convert milliseconds to seconds for 'fromtimestamp()', and then convert the seconds to local time
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    
    # Create timestamp column from original timestamp column
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # Extract columns to create time table. Use built-in functions to get the specific date/time fields from 'start_time'
    time_table = df.select('start_time').distinct()
    time_table = time_table.withColumn('hour', hour('start_time')).withColumn('day', dayofmonth('start_time')).withColumn('week', weekofyear('start_time')).withColumn('month', month('start_time')).withColumn('year', year('start_time')).withColumn('weekday', date_format(col("start_time"), "u"))
    
    # Write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data,'time.parquet'))

    # Read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data,'songs.parquet'))
    artists_df = spark.read.parquet(os.path.join(output_data,'artists.parquet'))

    # First create temp tables to query using Spark SQL
    df.createOrReplaceTempView("event_log")
    songs_df.createOrReplaceTempView("songs")
    artists_df.createOrReplaceTempView("artists")
    
    # Extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql('''
        SELECT
            EL.start_time,
            EL.userid AS user_id,
            EL.level,
            S.song_id,
            A.artist_id,
            EL.sessionId AS session_id,
            EL.location,
            EL.useragent AS user_agent
        FROM event_log EL
        LEFT JOIN songs S ON (EL.song = S.title) and (EL.length = S.duration)
        LEFT JOIN artists A on (EL.artist = A.name) and (S.artist_id = A.artist_id)''')

    # Write songplays table to parquet files partitioned by year and month
    # This requires joining with the 'time' data frame to get the year and month fields
    
    songplays_output = songplays_table.alias('songplays').join(time_table.alias('time'), col('songplays.start_time') == col('time.start_time'), 'inner').select(col('songplays.start_time'),col('songplays.user_id'),col('songplays.level'),col('songplays.song_id'),col('songplays.artist_id'),col('songplays.session_id'),col('songplays.location'),col('songplays.user_agent'),col('time.year'),col('time.month'))

    # Write the songplays to a parquet file
    songplays_output.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data,'songplays.parquet'))
    
def main():
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dataengineering-nano-dwh-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
