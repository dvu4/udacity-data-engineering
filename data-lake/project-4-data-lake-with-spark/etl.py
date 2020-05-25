import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session
    Put a config key spark.jars.packages using SparkSession
    Access S3 from Spark Cluster using Hadoop version 2.x or greater
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# https://issues.apache.org/jira/browse/SPARK-21752

# song_data is a subset of real data from the Million Song Dataset. 
# Each file is in JSON format and contains metadata about a song and the artist of that song. 
# The files are partitioned by the first three letters of each song's track ID.

# TRAAAAW128F429D538.json file looks like : 
#    "num_songs": 1, 
#    "artist_id": "ARD7TVE1187B99BFB1", 
#    "artist_latitude": null, 
#    "artist_longitude": null, 
#    "artist_location": "California - LA", 
#    "artist_name": "Casual", 
#    "song_id": "SOMZWCG12A8C13C480", 
#    "title": "I Didn't Mean To", 
#    "duration": 218.93179, 
#    "year": 0

def process_song_data(spark, input_data, output_data):
    """
    Process the song data files and extract the song and artist tables
        param spark :  Spark Context
        param input_data : input file path
        param output_data: output file path
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*json"
    
    # read song data file
    df = spark.read.json(song_data, mode="PERMISSIVE", columnNameOfCorruptRecord="broken").drop_duplicates()

    
    ########################  song_table ########################
    ##song_table includes (song_id, title, artist_id, year, duration)
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table.parquet", mode="overwrite", partitionBy=["year", "artist"])
    
    
    ########################  artist_table ########################
    ##artist_table_create includes (artist_id, name, location, latitude, longitude)
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table.parquet", mode="overwrite")

########################################################################################################################
    
# 2018-11-01-events.json file looks like : 
#{
#    "artist":"Barry Tuckwell\/Academy of St Martin-in-the-Fields\/Sir Neville Marriner",
#    "auth":"Logged In",
#    "firstName":"Celeste",
#    "gender":"F",
#    "itemInSession":1,
#    "lastName":"Williams",
#    "length":277.15873,
#    "level":"free",
#    "location":"Klamath Falls, OR",
#    "method":"PUT",
#    "page":"NextSong",
#    "registration":1541077528796.0,
#    "sessionId":438,"song":"Horn Concerto No. 4 in E flat K495: II. Romance (Andante cantabile)",
#    "status":200,
#    "ts":1541990264796,
#    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/37.0.2062.103 Safari\/537.36\"",
#    "userId":"53"
#} 



def process_log_data(spark, input_data, output_data):
    """
    Process the event log of Sparkify app usage and extract data for the user, time and songplays tables specifically for 'NextSong' event
        param spark :  Spark Context
        param input_data : input file path
        param output_data: output file path
    """

    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    ##log data includes ("artist", "auth","firstName","gender","lastName", "length", "level","location","registration", "sessionId","song", "status", "ts",  "userAgent", "userId")
    # read log data file
    df = spark.read.json(log_data, mode="PERMISSIVE", columnNameOfCorruptRecord="broken").drop_duplicates()
    
    
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    ########################  user_table ########################
    ##user_table_create includes (userId, firstName, lastName, gender, level)
    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), 
                            col("firstName").alias("first_name"), 
                            col("lastName").alias("last_name"), 
                            col("gender").alias("gender"), 
                            col("level").alias("level")
                           ).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table.parquet", mode="overwrite")


    ########################  time_table ########################
    ##time_table includes (start_time, hour, day, week, month, year, weekday)
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(int(ts)/1000),T.TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    

    # extract columns to create time table
    time_table = df.withColumn("hour",     F.hour("start_time"))\
                    .withColumn("day",     F.dayofmonth("start_time"))\
                    .withColumn("week",    F.weekofyear("start_time"))\
                    .withColumn("month",   F.month("start_time"))\
                    .withColumn("year",    F.year("start_time"))\
                    .withColumn("weekday", F.dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table.parquet", mode="overwrite", partitionBy = ["year", "month"])

    
    ######################## songplay_table  ########################
    ##songplay_table includes (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")
        
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.selectColumn("songplay_id", F.monotonically_increasing_id())\
                        .join(song_df, df.song == song_df.title, how="inner")\
                        .select("songplay_id", 
                                "start_time", 
                                col("userId").alias("user_id"),
                                "level", 
                                "song_id", 
                                "artist_id", 
                                col("sessionId").alias("session_id"), 
                                "location", 
                                col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table.parquet", mode = "overwrite", partitionBy = ["year", "month"])


    
    
def main():
    spark = create_spark_session()
    input_data = "s3://my-udacity-dend/"
    output_data = "s3://my-udacity-dend/output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()