import os
import pyspark
import requests
import webbrowser
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQueryException
import pandas as pd
import numpy as np

import io
import base64
import traceback

import matplotlib.pyplot as plt
import seaborn as sns

from bokeh.io import curdoc
from bokeh.layouts import column, gridplot
from bokeh.models import ColumnDataSource, NumeralTickFormatter, HoverTool
from bokeh.plotting import figure, show
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.palettes import Spectral10
from bokeh.transform import factor_cmap
import plotly.graph_objs as go
import plotly.express as px

from tornado.ioloop import IOLoop
from tornado.netutil import bind_sockets

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

import mpld3

pd.set_option('display.max_colwidth', None)



spark = SparkSession.builder.appName("FactsSpark").enableHiveSupport().getOrCreate()
spark

spark.sql('''set hive.exec.dynamic.partition=true''')

spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict''')

spark.sql("CREATE DATABASE IF NOT EXISTS facts")

spark.sql('''
CREATE EXTERNAL TABLE IF NOT EXISTS facts.tweet_engagement_processed (
tweet_id STRING,
like_count INT,
retweet_count INT, 
reply_count INT,
qoute_count INT,
impression_count INT,
hashtags_count INT,
media_count INT
)
LOCATION 'twitter-processed-data.tweet_engagement_processed'  ''')



spark.sql('''
CREATE EXTERNAL TABLE IF NOT EXISTS facts.user_activity_processed (
user_id STRING,
tweet_count INT,
followers_count INT, 
following_count INT
)
LOCATION 'twitter-processed-data.user_activity_processed'  ''')


spark.sql('''
CREATE EXTERNAL TABLE IF NOT EXISTS facts.users_tweets_processed (
tweet_id STRING,
user_id STRING,
num_hashtags INT,
num_media INT
)
LOCATION 'twitter-processed-data.users_tweets_processed'  ''')


spark.sql('''
CREATE EXTERNAL TABLE IF NOT EXISTS facts.totals_mertics_processed (
tweet_id STRING,
user_id STRING,
place_id STRING,
media_id STRING,
count_weekend_tweets INT,
count_week_tweets INT,
verified_user_count INT,
protected_user_count INT,
high_popularity_user_count INT,
middle_popularity_user_count INT,
low_popularity_user_count INT,
photo_media_count INT,
video_media_count INT,
city_count INT,
limited_reply_tweet_count INT
)
LOCATION 'twitter-processed-data.totals_mertics_processed'  ''')



spark.sql('''

INSERT INTO TABLE facts.tweet_engagement_processed
SELECT
    DISTINCT t.tweet_id,
    t.like_count,
    t.retweet_count,
    t.reply_count,
    t.qoute_count,
    t.imperssion_count,
    SIZE(t.hashtags_list),
    SIZE(t.media_ids)
FROM twitter_raw_data.tweet_data_raw t
WHERE NOT EXISTS (
    SELECT * FROM facts.tweet_engagement_processed f
    WHERE f.tweet_id = t.tweet_id )
''')


spark.sql('''

INSERT INTO TABLE facts.user_activity_processed
SELECT
    DISTINCT u.user_id,
    u.tweet_count,
    u.followers_count,
    u.following_count
FROM twitter_raw_data.user_data_raw u
WHERE NOT EXISTS (
    SELECT * FROM facts.user_activity_processed f
    WHERE f.user_id = u.user_id )
''')

spark.sql('''
INSERT INTO TABLE facts.users_tweets_processed
SELECT
    t.tweet_id,
    u.user_id,
    size(t.hashtags_list) as num_hashtags,
  sum(case when m.media_id is not null then 1 else 0 end) as num_media
FROM twitter_raw_data.tweet_data_raw t
LEFT JOIN twitter_raw_data.user_data_raw u ON t.author_id = u.user_id
LEFT JOIN twitter_raw_data.media_data_raw m ON array_contains(t.media_ids, m.media_id)
GROUP BY
 t.tweet_id,
 t.hashtags_list,
 u.user_id,
 m.media_id
''')

spark.sql('''
INSERT INTO TABLE facts.totals_mertics_processed
SELECT
    null as tweet_id,
    null as user_id,
    null as place_id,
    null as media_id,
    COUNT(DISTINCT CASE WHEN dayofweek(from_unixtime(UNIX_TIMESTAMP(t.tweet_date, 'yyyy-MM-dd HH:mm:ss'))) in (6,7) THEN t.tweet_id END) AS count_weekend_tweets,
    COUNT(DISTINCT CASE WHEN dayofweek(from_unixtime(UNIX_TIMESTAMP(t.tweet_date, 'yyyy-MM-dd HH:mm:ss'))) not in (6,7) THEN t.tweet_id END) AS count_week_tweets,
    COUNT(DISTINCT CASE WHEN u.verfied = true THEN u.user_id ELSE NULL END) AS verified_user_count,
    COUNT(DISTINCT CASE WHEN u.protected = true THEN u.user_id ELSE NULL END) AS protected_user_count,
    COUNT(DISTINCT CASE WHEN u.user_popularity = 'High' THEN u.user_id ELSE NULL END) AS high_popularity_user_count,
    COUNT(DISTINCT CASE WHEN u.user_popularity = 'Middle' THEN u.user_id ELSE NULL END) AS middle_popularity_user_count,
    COUNT(DISTINCT CASE WHEN u.user_popularity = 'Low' THEN u.user_id ELSE NULL END) AS low_popularity_user_count,
    COUNT(DISTINCT CASE WHEN m.media_type = 'photo' THEN m.media_id ELSE NULL END) AS photo_media_count,
    COUNT(DISTINCT CASE WHEN m.media_type = 'video' THEN m.media_id ELSE NULL END) AS video_media_count,
    COUNT(DISTINCT CASE WHEN p.place_type = 'city' THEN p.place_id ELSE NULL END) AS city_count,
    COUNT(DISTINCT CASE WHEN t.who_can_reply != 'everyone' THEN t.tweet_id ELSE NULL END) AS limited_reply_tweet_count
FROM
    twitter_raw_data.tweet_data_raw t
    LEFT JOIN twitter_raw_data.user_data_raw u ON t.author_id = u.user_id
    LEFT JOIN twitter_raw_data.media_data_raw m ON array_contains(t.media_ids, m.media_id)
    LEFT JOIN twitter_raw_data.place_data_raw p ON t.tweet_location = p.place_id
''')




hash_df = spark.sql("SELECT hashtags_list FROM twitter_raw_data.tweet_data_raw WHERE SIZE(hashtags_list) > 0")

hash_counts = (
    hash_df.select(explode("hashtags_list").alias("hashtag"))
      .groupBy("hashtag")
      .agg(count("*").alias("count"))
)


top_hash = hash_counts.orderBy("count", ascending=False).limit(10).collect()



popular_users = spark.table("twitter_raw_data.user_data_raw") \
    .select("username", col("followers_count").cast("int")) \
    .groupBy("username") \
    .agg({"followers_count": "max"}) \
    .withColumnRenamed("max(followers_count)", "followers_count") \
    .orderBy(desc("followers_count")) \
    .limit(10) \
    .toPandas()


# Convert tweet_data_raw to a Spark DataFrame
tweet_data = spark.sql("SELECT date_format(from_unixtime(UNIX_TIMESTAMP(tweet_date, 'yyyy-MM-dd HH:mm:ss')), 'EEEE') as week_day FROM twitter_raw_data.tweet_data_raw")


# Group by week_day and count the number of tweets
tweet_counts = tweet_data.groupBy("week_day").count()

# Convert to a Pandas DataFrame
tweet_counts_pd = tweet_counts.toPandas()

# Define the order of weekdays
weekdays_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


media_counts = spark.sql("SELECT media_type, COUNT(media_type) as count FROM twitter_raw_data.media_data_raw WHERE media_type != 'NaN' GROUP BY media_type")


# Read the data from Hive table into a Pandas DataFrame
user_data_raw_df = spark.sql("SELECT user_popularity, COUNT(*) AS count FROM twitter_raw_data.user_data_raw WHERE user_popularity != 'null' GROUP BY user_popularity")


# Create a Bokeh figure for the first chart
top_hash_pd_source = ColumnDataSource(top_hash_pd)
p1 = figure(x_range=top_hash_pd["hashtag"], plot_height=500, plot_width=1000, title="Top 10 Hashtags and Their Counts")
p1.vbar(x="hashtag", top="count", width=0.9, source=top_hash_pd_source, line_color="white", fill_color=factor_cmap('hashtag', palette=Spectral10, factors=top_hash_pd['hashtag']))

# Add hover tool
hover = HoverTool(tooltips=[("Hashtag", "@hashtag"), ("Count", "@count")])
p1.add_tools(hover)


# Create a Bokeh figure for the second chart
popular_users_source = ColumnDataSource(popular_users)
p2 = figure(x_range=popular_users["username"], plot_height=500, plot_width=1000, title="Top 10 Popular Users Based on Follower Count")
p2.vbar(x="username", top="followers_count", width=0.9, source=popular_users_source, line_color="white", fill_color=factor_cmap('username', palette=Spectral10, factors=popular_users['username']))

# Format y-axis ticks as thousands
p2.yaxis.formatter = NumeralTickFormatter(format='0.0a')


# Add hover tool
hover = HoverTool(tooltips=[("Username", "@username"), ("Follower Count", "@followers_count")])
p2.add_tools(hover)



# Create a Bokeh figure for the third chart
p3 = figure(x_range=weekdays_order, plot_height=500, plot_width=1000, title="Number of Tweets by Weekday")
p3.vbar(x="week_day", top="count", width=0.9, source=tweet_counts_pd, line_color="white", fill_color=factor_cmap('week_day', palette=Spectral10, factors=weekdays_order))

# Add hover tool
hover = HoverTool(tooltips=[("Weekday", "@week_day"), ("Count", "@count")])
p3.add_tools(hover)


# Create a Bokeh figure for the forth chart
media_counts_pd_source = ColumnDataSource(media_counts.toPandas())
p4 = figure(x_range=media_counts_pd_source.data["media_type"], plot_height=500, plot_width=1000, title="Count of Each Media Type")
p4.vbar(x="media_type", top="count", width=0.9, source=media_counts_pd_source, line_color="white", fill_color=factor_cmap('media_type', palette=colors, factors=media_counts_pd_source.data["media_type"]))

# Add hover tool
hover = HoverTool(tooltips=[("Media Type", "@media_type"), ("Count", "@count")])
p4.add_tools(hover)



# Create a Bokeh figure for the fifth chart
user_data_pd = user_data_raw_df.toPandas()

# Create a Bokeh figure
user_data_source = ColumnDataSource(user_data_pd)
p5 = figure(x_range=user_data_pd["user_popularity"], plot_height=500, plot_width=1000, title="Count of User Popularity")
p5.vbar(x="user_popularity", top="count", width=0.9, source=user_data_source, line_color="white", fill_color=factor_cmap('user_popularity', palette=Spectral10, factors=user_data_pd['user_popularity']))

# Add hover tool
hover = HoverTool(tooltips=[("User Popularity", "@user_popularity"), ("Count", "@count")], formatters={'@count': 'printf'})
p5.add_tools(hover)



# Create a grid layout for the charts
layout = gridplot([[p1], [p2], [p3], [p4],[p5]])


# Save the Bokeh figures to an HTML file
from bokeh.resources import CDN
from bokeh.embed import file_html
html = file_html(layout, CDN, "Twitter Charts")
with open("Dashboard/twitter_charts.html", "w") as f:
    f.write(html)
    
    
# Open the file in a new browser window
webbrowser.open_new_tab('Dashboard/twitter_charts.html')

# stop the SparkSession
spark.stop()

