import os
import pyspark
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQueryException
import pandas as pd
pd.set_option('display.max_colwidth', None)
import json
from ast import literal_eval
import ast
import time
import numpy as np

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

spark = SparkSession.builder.appName("TwitterStream").enableHiveSupport().getOrCreate()
spark

sc = spark.sparkContext
sc.setLogLevel("ERROR")

lines = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 7777).load()

time.sleep(10)

# Write the tweets to the "tweetquery" memory table
writeTweet = lines.writeStream.\
    outputMode("append"). \
    format("memory"). \
    queryName("tweetquery"). \
    trigger(processingTime='2 seconds'). \
    start()

print("----- streaming is running -------")

time.sleep(20)

data_schema = StructType([
    StructField('reply_settings', StringType(), True),
    StructField('edit_history_tweet_ids', ArrayType(StringType()), True),
    StructField('referenced_tweets', ArrayType(MapType(StringType(), StringType())), True),
    StructField('created_at', StringType(), True),
    StructField('lang', StringType(), True),
    StructField('text', StringType(), True),
    StructField('conversation_id', StringType(), True),
    StructField('author_id', StringType(), True),
    StructField('id', StringType(), True),
    StructField('batch_id', IntegerType(), True),
    StructField('hashtags', ArrayType(StringType()), True),
    StructField('attachments.media_keys', ArrayType(StringType()), True),
    StructField('public_metrics.retweet_count', LongType(), True),
    StructField('public_metrics.reply_count', LongType(), True),
    StructField('public_metrics.like_count', LongType(), True),
    StructField('public_metrics.quote_count', LongType(), True),
    StructField('public_metrics.impression_count', LongType(), True),
    StructField('entities.mentions', ArrayType(MapType(StringType(), StringType())), True),
    StructField('user.profile_image_url', StringType(), True),
    StructField('user.pinned_tweet_id', StringType(), True),
    StructField('user.location', StringType(), True),
    StructField('user.protected', BooleanType(), True),
    StructField('user.verified', BooleanType(), True),
    StructField('user.description', StringType(), True),
    StructField('user.name', StringType(), True),
    StructField('user.id', StringType(), True),
    StructField('user.username', StringType(), True),
    StructField('user.created_at', StringType(), True),
    StructField('user.public_metrics.followers_count', LongType(), True),
    StructField('user.public_metrics.following_count', LongType(), True),
    StructField('user.public_metrics.tweet_count', LongType(), True),
    StructField('user.public_metrics.listed_count', LongType(), True),
    StructField('media.media_key', StringType(), True),
    StructField('media.type', StringType(), True),
    StructField('in_reply_to_user_id', StringType(), True),
    StructField('geo.place_id', StringType(), True),
    StructField('place.name', StringType(), True),
    StructField('place.place_type', StringType(), True),
    StructField('place.id', StringType(), True),
    StructField('place.country_code', StringType(), True),
    StructField('place.full_name', StringType(), True),
    StructField('place.country', StringType(), True),
    StructField('place.geo.type', StringType(), True),
    StructField('place.geo.bbox', StringType(), True)
])



try:
    
    print("----- receiving the data -------")
    
    # Define an empty dataframe to hold the normalized data
    df_normalized = pd.DataFrame()
    
    max_id = 0
    
    new_order = ['reply_settings', 'edit_history_tweet_ids', 'referenced_tweets', 'created_at', 'lang', 'text', 'conversation_id', 'author_id', 'id', 'batch_id', 'hashtags', 'attachments.media_keys', 'public_metrics.retweet_count',\
            'public_metrics.reply_count','public_metrics.like_count','public_metrics.quote_count','public_metrics.impression_count','entities.mentions','user.profile_image_url','user.pinned_tweet_id','user.location',\
            'user.protected','user.verified','user.description','user.name','user.id','user.username','user.created_at','user.public_metrics.followers_count','user.public_metrics.following_count','user.public_metrics.tweet_count',\
            'user.public_metrics.listed_count','media.media_key','media.type','in_reply_to_user_id','geo.place_id','place.name','place.place_type','place.id','place.country_code','place.full_name','place.country','place.geo.type','place.geo.bbox']
    
    hdfs_path = "/user/itversity/twitter-landing-data"
    


    # Continuously read data from the stream and append to the dataframe
    
        # Read the tweets from the "tweetquery" memory table and show the result
    result_df = spark.sql("SELECT * FROM tweetquery").toPandas()

        # Assuming `result_df` contains the dataframe with the JSON strings
        # Convert each JSON string to a Python dictionary
    result_df['value'] = result_df['value'].apply(json.loads)

        # Normalize the dictionary data to a dataframe
    new_normalized_df = pd.json_normalize(result_df['value'])
        
        # Filter out data that has already been appended
    final_normalized_df = new_normalized_df[new_normalized_df['batch_id'] > max_id]

        # Append the new data to the existing dataframe    
    df_normalized = df_normalized.append(final_normalized_df)
        
    max_id = df_normalized["batch_id"].max()

    df_normalized = df_normalized.reindex(columns=new_order)
        
    
    print("----- cleaning the data -------")
        
        # change the dataframe coulmns datatypes to convert it into spark dataframe
        
    df_normalized['referenced_tweets'] = df_normalized['referenced_tweets'].apply(lambda x: [str(tweet) for tweet in x] if isinstance(x, list) else [])
    df_normalized['referenced_tweets'] = df_normalized['referenced_tweets'].apply(lambda x: [json.loads(tweet.replace("'", "\"")) for tweet in x] if isinstance(x, list) else [])
    df_normalized['attachments.media_keys'] = df_normalized['attachments.media_keys'].apply(lambda x: [] if isinstance(x, float) and np.isnan(x) else x)
    df_normalized['user.pinned_tweet_id'] = df_normalized['user.pinned_tweet_id'].apply(lambda x: None if x == [] else x)
    df_normalized['attachments.media_keys'] = df_normalized['attachments.media_keys'].fillna(value={})
        # Fill null values with an empty dictionary
    df_normalized['entities.mentions'] = df_normalized['entities.mentions'].fillna(value={})
        # Convert string values to dictionary values
    df_normalized['entities.mentions'] = df_normalized['entities.mentions'].apply(lambda x: [ast.literal_eval(mention) if isinstance(mention, str) else mention for mention in x] if isinstance(x, list) else [])
        
    df_normalized['text'] = df_normalized['text'].apply(lambda s: s.replace('\n', ' '))
    df_normalized['user.description'] = df_normalized['user.description'].apply(lambda s: s.replace('\n', ' '))


        # Create Spark DataFrame
    spark_df = spark.createDataFrame(df_normalized, schema=data_schema)
        
    spark_df = spark_df.withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        
        # Extract Year, Month, Day, and Hour from created_at column
    final_spark_df = spark_df.withColumn("Year", year("created_at")) \
        .withColumn("Month", month("created_at")) \
        .withColumn("Day", dayofmonth("created_at")) \
        .withColumn("Hour", hour("created_at"))
    
    print("----- writing data in the hdfs -------")
        
        # Convert DataFrame to Spark DataFrame and write to HDFS partitioned by year, month, day, and hour
    final_spark_df.write.partitionBy("year", "month", "day", "hour").mode("overwrite").parquet("twitter-landing-data")
        
         # Load the partitioned data into a Spark DataFrame
    hive_df = spark.read.parquet(hdfs_path+"/*/*/*/*")
        
                
        # Create the Hive table
    final_hive_df= hive_df.withColumnRenamed("attachments.media_keys", "attachments_media_keys")\
                    .withColumnRenamed("public_metrics.retweet_count", "public_metrics_retweet_count")\
                    .withColumnRenamed("public_metrics.reply_count", "public_metrics_reply_count")\
                    .withColumnRenamed("public_metrics.like_count", "public_metrics_like_count")\
                    .withColumnRenamed("public_metrics.quote_count", "public_metrics_quote_count")\
                    .withColumnRenamed("public_metrics.impression_count", "public_metrics_impression_count")\
                    .withColumnRenamed("entities.mentions", "entities_mentions")\
                    .withColumnRenamed("user.profile_image_url", "user_profile_image_url")\
                    .withColumnRenamed("user.pinned_tweet_id", "user_pinned_tweet_id")\
                    .withColumnRenamed("user.location", "user_location")\
                    .withColumnRenamed("user.protected", "user_protected")\
                    .withColumnRenamed("user.verified", "user_verified")\
                    .withColumnRenamed("user.description", "user_description")\
                    .withColumnRenamed("user.name", "user_name")\
                    .withColumnRenamed("user.id", "user_id")\
                    .withColumnRenamed("user.username", "user_username")\
                    .withColumnRenamed("user.created_at", "user_created_at")\
                    .withColumnRenamed("user.public_metrics.followers_count", "user_public_metrics_followers_count")\
                    .withColumnRenamed("user.public_metrics.following_count", "user_public_metrics_following_count")\
                    .withColumnRenamed("user.public_metrics.tweet_count", "user_public_metrics_tweet_count")\
                    .withColumnRenamed("user.public_metrics.listed_count", "user_public_metrics_listed_count")\
                    .withColumnRenamed("media.media_key", "media_media_key")\
                    .withColumnRenamed("media.type", "media_type")\
                    .withColumnRenamed("geo.place_id", "geo_place_id")\
                    .withColumnRenamed("place.name", "place_name")\
                    .withColumnRenamed("place.place_type", "place_place_type")\
                    .withColumnRenamed("place.id", "place_id")\
                    .withColumnRenamed("place.country_code", "place_country_code")\
                    .withColumnRenamed("place.full_name", "place_full_name")\
                    .withColumnRenamed("place.country", "place_country")\
                    .withColumnRenamed("place.geo.type", "place_geo_type")\
                    .withColumnRenamed("place.geo.bbox", "place_geo_bbox")
        
        
    final_hive_df.write.mode("append").saveAsTable("twitter_landing_table")
        


        # Wait for a few seconds before reading the stream again
        
        
    print("----- waiting for the next batch -------")
    
    time.sleep(100)
        
except KeyboardInterrupt:
    # Stop the streaming query when the socket is closed from the Python listener
    writeTweet.stop()
    print("Streaming stopped due to keyboard interrupt")
    
except StreamingQueryException:
    # The socket was closed, stop the query
    writeTweet.stop()
    print("Streaming stopped due to socket closure")
    
except ConnectionResetError:
    # Break the loop when the connection with the port is interrupted
    print("Connection with port interrupted, stopping the loop.")
    writeTweet.stop()
    
# Close the streaming

writeTweet.awaitTermination()

writeTweet.stop()

# stop the SparkSession
spark.stop()

#Stop the context

sc.stop()


