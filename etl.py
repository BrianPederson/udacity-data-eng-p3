# etl.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/18/2020
# PURPOSE: Script to implement ETL processes to populate 'data lake' tables for Data Engineering Project 3.
#
# Included functions:
#     get_input_args    - function to process input arguments specific to train.py
#     process_song_data - extract/insert data for songs and artists dimensions from source song json files
#     process log_data  - extract/insert data for users and time dimensions; songplays fact from source log json files
#     main              - main function performs ETL
#
# Parameters: see parameters of get_input_args function.
#

import os
import time
import configparser
import sys
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, LongType, DateType, TimestampType


def create_spark_session():
    """
    Utility function to create a Spark session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")   # INFO, ERROR

    return spark


def process_song_data(spark, input_data, output_data, input_song_pattern):
    """
    Extract data for song and artist dimensions from source song json files then insert into parquet files
    Parameters:
      spark - Spark session
      input_data - filepath to source json files
      output_data - filepath to target parquet files
      input_song_pattern - file pattern for input song files
    """

    # get filepath to song data file
    song_data = input_data + input_song_pattern
    print("Processing song source data: " + song_data)

    # this is not necessary but useful as a code sample for future reference
    songSchema = StructType([StructField("artist_id", StringType()),
                             StructField("artist_latitude", DoubleType()),
                             StructField("artist_location", StringType()),
                             StructField("artist_longitude", DoubleType()),
                             StructField("artist_name", StringType()),
                             StructField("duration", DoubleType()),
                             StructField("num_songs", IntegerType()),
                             StructField("song_id", StringType()),
                             StructField("title", StringType()),
                             StructField("year", IntegerType())
                            ])

    # read song/artist source data file
    dfSongSource = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs dataframe (pre songs table)
    # use Spark SQL query to create songs dataframe (proto songs table)
    dfSongSource.createOrReplaceTempView("staging_songs")

    songs_table = spark.sql(
    """
    SELECT song_id,
           MIN(title) AS title,
           MIN(artist_id) AS artist_id,
           MIN(year) AS year,
           MIN(duration) AS duration
      FROM staging_songs
     GROUP BY song_id
    """)

    # add unknown dummy row to songs dataframe
    unknownSongRow = spark.createDataFrame([('***UNKNOWN_SONG***', '***Unknown Song***', '***UNKNOWN_ARTIST***', 0, 0)])
    songs_table = songs_table.union(unknownSongRow)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").format("parquet").mode("overwrite").save(output_data + "songs.parquet")
    print("Processed songs dimension")

    # extract columns to create artists table
    # use Spark SQL query to create artists dataframe (proto artists table)
    dfSongSource.createOrReplaceTempView("staging_songs")

    artists_table = spark.sql(
    """
    SELECT artist_id,
           MIN(artist_name) AS name,
           MIN(artist_location) AS location,
           MIN(artist_latitude) AS latitude,
           MIN(artist_longitude) AS longitude
      FROM staging_songs
     GROUP BY artist_id
    """)

    # add unknown dummy row to artists dataframe
    unknownArtistRow = spark.createDataFrame([('***UNKNOWN_ARTIST***', '*** Unknown Artist ***', '', 0.0, 0.0)])
    artists_table = artists_table.union(unknownArtistRow)

    # write artists table to parquet files
    artists_table.write.format("parquet").mode("overwrite").save(output_data + "artists.parquet")
    print("Processed artists dimension")

    # extract columns to create song keys file dataframe (proto song_keys table)
    song_keys_table = dfSongSource.select(["song_id", "title", "duration", "artist_id", "artist_name"]).dropDuplicates()

    # write song keys table to parquet files
    song_keys_table.write.format("parquet").mode("overwrite").save(output_data + "song_keys.parquet")
    print("Processed song keys table")


def process_log_data(spark, input_data, output_data, input_log_pattern, mode):
    """
    Extract data for users, time dimensions and songplays fact from source log json files then insert into parquet files
    Parameters:
      spark - Spark session
      input_data - filepath to source json files
      output_data - filepath to target parquet files
      input_log_pattern - file pattern for input log files
    """

    # get filepath to log data file
    log_data = input_data + input_log_pattern
    print("Processing log source data: " + log_data)

    # read log data file
    dfLogSource = spark.read.json(log_data)

    # filter log data by actions for song plays
    dfLogSource = dfLogSource.where(dfLogSource.page == 'NextSong')   # filter dataframe to only include rows with page == 'NextSong'

    # convert userID from string to integer
    dfLogSource = dfLogSource.withColumn('userID', expr("cast(userID as int)"))

    # extract columns for users table
    # use Spark SQL query to create users dataframe (proto users table)
    dfLogSource.createOrReplaceTempView("staging_log")

    users_table = spark.sql(
    """
    SELECT userID AS user_id,
           MIN(firstName) AS first_name,
           MIN(lastName) AS last_name,
           MIN(gender) AS gender,
           MIN(level) AS level
      FROM staging_log
     GROUP BY userID
    """)

    # write users table to parquet files
    users_table.write.format("parquet").mode("overwrite").save(output_data + "users.parquet")
    print("Processed users dimension")

    # create timestamp column start_time from original timestamp column ts NOT using udf()
    # note that this method has a side effect of stripping microseconds from start_time timestamp
    dfLogSource = dfLogSource.withColumn("start_time", to_timestamp(from_unixtime(dfLogSource.ts/1000)))

    # extract columns to create time table
    # use Spark SQL query to create time dataframe (pre time table)
    dfLogSource.createOrReplaceTempView("staging_log")

    time_table = spark.sql(
    """
    SELECT DISTINCT start_time,
                    hour(start_time)       AS hour,
                    day(start_time)        AS day,
                    weekofyear(start_time) AS week,
                    month(start_time)      AS month,
                    year(start_time)       AS year,
                    dayofweek(start_time)  AS weekday
      FROM staging_log
     WHERE 1=1
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").mode("overwrite").save(output_data + "time.parquet")
    print("Processed time dimension")

    # enhance log source to include synthetic primary key songplay_id
    get_songplay_id = udf(lambda x, y, z: (f"{x:06}.{y:06}.{z}"), StringType())

    dfLogSource = dfLogSource.withColumn("songplay_id", get_songplay_id(dfLogSource.userID, dfLogSource.sessionId, dfLogSource.ts) )

    # enhance log source to include hour and year to facilitate partitioning below
    dfLogSource = dfLogSource.withColumn("year", year(dfLogSource.start_time)) \
                             .withColumn("month", month(dfLogSource.start_time))

    # read in song keys data to use for songplays table
    song_keys_table = spark.read.parquet(output_data + "song_keys.parquet")

    # register temp view for song_keys utility table
    song_keys_table.createOrReplaceTempView("song_keys")

    # extract columns from joined song, artists and log datasets to create songplays table
    # use Spark SQL query to create songplays dataframe (proto songplays table)
    dfLogSource.createOrReplaceTempView("staging_log")

    songplays_table = spark.sql(
    """
    SELECT e.songplay_id,
           e.start_time,
           e.year,
           e.month,
           e.userID as user_id,
           e.level,
           COALESCE(s.song_id, '***UNKNOWN_SONG***') as song_id,
           COALESCE(s.artist_id, '***UNKNOWN_ARTIST***') as artist_id,
           e.sessionid as session_id,
           e.location,
           e.useragent as user_agent
      FROM staging_log e
      LEFT OUTER JOIN song_keys s ON e.song = s.title AND e.artist = s.artist_name and e.length = s.duration
     WHERE e.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    # note the mode can vary between overwrite and append based on command line parm
    songplays_table.write.partitionBy("year", "month").format("parquet").mode(mode).save(output_data + "songplays.parquet")
    print("Processed songplays fact")


def get_input_args():
    """
    Retrieves and parses the 5 optional arguments provided by the user.

    Mandatory command line arguments:
      None
    Optional command line arguments:
      1. Path for input bucket/directory.
      2. File pattern for input song files.
      3. File pattern for input log files.
      4. Path for output bucket/directory.
      5. Mode for songplays fact table (overwrite vs append)

    This function returns these arguments as an ArgumentParser object.
    Parameters:
      None - using argparse module to create & store command line arguments
    Returns:
      parser.namespace - data structure that stores the command line arguments object
    """
    # Create Parse using ArgumentParser
    parser = argparse.ArgumentParser()
    parser.prog = 'etl.py'
    parser.description = "Performs ETL functions for Sparkify Data Lake."

    # Argument 1
    parser.add_argument('--input_path', type = str, default="s3a://udacity-dend/",
               help = "Path for input bucket or directory (e.g. 's3a://udacity-dend/')")
    # Argument 2
    parser.add_argument('--input_song_pattern', type = str, default='song_data/*/*/*/*.json',
               help = "File pattern for song json files (e.g. 'song_data/*/*/*/*.json', 'song_data/A/A/*/*.json', 'song_data/A/A/A/*.json')")
    # Argument 3
    parser.add_argument('--input_log_pattern', type = str, default='log_data/*/*/*.json',
               help = "File pattern for log json files (e.g. 'log_data/*/*/*.json', 'log_data/*/*/2018-11-0*.json', 'log_data/*/*/2018-11-01-events.json')")
    # Argument 4
    parser.add_argument('--output_path', type = str, required=True, # default="analytics/",
               help = "Path for output bucket or directory (e.g. 's3a://sparkify-bp/analytics/', 'analytics/')")
    # Argument 5:
    parser.add_argument('--mode', type = str, choices = ['overwrite', 'append'], default="overwrite",
               help = "Mode to update songplays fact table ('overwrite', 'append')")

    # Note: this will perform system exit if argument is malformed or imcomplete
    in_args = parser.parse_args()

    # return parsed argument collection
    return in_args


def main():
    """
    Main function contains core logic for ETL.

    Parameters: see parameters of get_input_args function.
    """

    # configuration processing
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    # command line argument processing
    in_args = get_input_args()
    print(in_args)    # temp debug

    start_time = time.time()

    spark = create_spark_session()

    process_song_data(spark, in_args.input_path, in_args.output_path, in_args.input_song_pattern)
    process_log_data(spark, in_args.input_path, in_args.output_path, in_args.input_log_pattern, in_args.mode)

    print("** Total Elapsed Runtime: " + time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)) )


if __name__ == "__main__":
    main()

#python etl.py --input_song_pattern song_data/A/A/A/*.json --input_log_pattern log_data/*/*/2018-11-01*.json --output_path s3a://sparkify-bp/analytics/ --mode overwrite
#python etl.py --input_path data/ --input_song_pattern song_data/A/*/*/*.json --input_log_pattern log_data/*/*/*.json --output_path analytics3/

