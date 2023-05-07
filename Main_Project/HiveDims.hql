SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Create a new database
CREATE DATABASE IF NOT EXISTS twitter_raw_data;


-- Create a new table to store the tweet_data dimension
CREATE EXTERNAL TABLE IF NOT EXISTS twitter_raw_data.tweet_data_raw (
tweet_id STRING,
author_id STRING,
tweet_text STRING,
hashtags_list ARRAY<STRING>,
retweet_count INT,
reply_count INT,
like_count INT,
qoute_count INT,
imperssion_count INT,
language STRING,
tweet_date TIMESTAMP,
who_can_reply STRING,
media_ids ARRAY<STRING>,
tweet_location STRING,
edit_history_ids ARRAY<STRING>
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
LOCATION 'twitter-raw-data.tweet_data_raw';


-- Create a new table to store the user_data dimension
CREATE EXTERNAL TABLE IF NOT EXISTS twitter_raw_data.user_data_raw (
user_id STRING,
username STRING,
name STRING,
profile_description STRING,
location STRING,
profile_image STRING,
pinned_tweet_id STRING,
profile_creation_date STRING,
protected BOOLEAN,
verfied BOOLEAN,
followers_count INT,
following_count INT,
tweet_count INT,
listed_count INT,
user_popularity STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
LOCATION 'twitter-raw-data.user_data_raw';


-- Create a new table to store the place_data dimension
CREATE EXTERNAL TABLE IF NOT EXISTS twitter_raw_data.place_data_raw (
place_id STRING,
place_name STRING,
place_fullname STRING,
place_country STRING,
place_country_code STRING,
place_type STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
LOCATION 'twitter-raw-data.place_data_raw';


-- Create a new table to store the media_data dimension
CREATE EXTERNAL TABLE IF NOT EXISTS twitter_raw_data.media_data_raw (
media_id STRING,
media_type STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
LOCATION 'twitter-raw-data.media_data_raw';

------------------------------------------------------------------------------------------------------------------------------------------------

MSCK REPAIR TABLE twitter_raw_data.tweet_data_raw;
MSCK REPAIR TABLE twitter_raw_data.user_data_raw;
MSCK REPAIR TABLE twitter_raw_data.place_data_raw;
MSCK REPAIR TABLE twitter_raw_data.media_data_raw;

-----------------------------------------------------------insertion----------------------------------------------------------------------------

-- Insert the tweet dimensions into the tweet_data table
INSERT INTO TABLE twitter_raw_data.tweet_data_raw PARTITION (year, month, day, hour)
SELECT
    DISTINCT t.id AS tweet_id,
    t.author_id AS author_id,
    t.text AS tweet_text,
    t.hashtags AS hashtags_list,
    t.public_metrics_retweet_count AS retweet_count,
    t.public_metrics_reply_count AS reply_count,
    t.public_metrics_like_count AS like_count,
    t.public_metrics_quote_count AS qoute_count,
    t.public_metrics_impression_count AS imperssion_count,
    t.lang AS language,
    t.created_at AS tweet_date,
    t.reply_settings AS who_can_reply,
    t.attachments_media_keys AS media_ids,
    t.geo_place_id AS tweet_location,
    t.edit_history_tweet_ids AS edit_history_ids,
    year(t.created_at) AS year,
    month(t.created_at) AS month,
    day(t.created_at) AS day,
    hour(t.created_at) AS hour
FROM DEFAULT.twitter_landing_table t
WHERE NOT EXISTS (
    SELECT * FROM twitter_raw_data.tweet_data_raw tr
    WHERE tr.tweet_id = t.id 
        AND tr.year = YEAR(t.created_at)
        AND tr.month = MONTH(t.created_at) 
        AND tr.day = DAY(t.created_at)
        AND tr.hour = HOUR(t.created_at)
);



-- Insert the tweet dimensions into the user_data table

INSERT INTO TABLE twitter_raw_data.user_data_raw PARTITION (year, month, day, hour)
SELECT
    DISTINCT t.user_id AS user_id,
    t.user_username AS username,
    t.user_name AS name,
    t.user_description AS profile_description,
    t.user_location AS location,
    t.user_profile_image_url AS profile_image,
    t.user_pinned_tweet_id AS pinned_tweet_id,
    t.user_created_at AS profile_creation_date,
    t.user_protected AS protected,
    t.user_verified AS verfied,
    t.user_public_metrics_followers_count AS followers_count,
    t.user_public_metrics_following_count AS following_count,
    t.user_public_metrics_tweet_count AS tweet_count,
    t.user_public_metrics_listed_count AS listed_count,
    CASE 
        WHEN t.user_public_metrics_followers_count > 5000 THEN 'High'
        WHEN t.user_public_metrics_followers_count >= 1000 AND t.user_public_metrics_followers_count <= 5000 THEN 'Middle'
        ELSE 'Low' 
        END AS user_popularity,
    YEAR(t.created_at) AS year,
    MONTH(t.created_at) AS month,
    DAY(t.created_at) AS day,
    HOUR(t.created_at) AS hour
FROM DEFAULT.twitter_landing_table t
WHERE NOT EXISTS (
    SELECT * FROM twitter_raw_data.user_data_raw tr
    WHERE tr.user_id = t.user_id AND tr.year = YEAR(t.created_at)
        AND tr.month = MONTH(t.created_at) AND tr.day = DAY(t.created_at)
        AND tr.hour = HOUR(t.created_at));



-- Insert the tweet dimensions into the place_data_raw table
INSERT INTO TABLE twitter_raw_data.place_data_raw PARTITION (year, month, day, hour)
SELECT
    DISTINCT t.place_id AS place_id,
    t.place_name AS place_name,
    t.place_full_name AS place_fullname,
    t.place_country AS place_country,
    t.place_country_code AS place_country_code,
    t.place_place_type AS place_type,
    YEAR(t.created_at) AS year,
    MONTH(t.created_at) AS month,
    DAY(t.created_at) AS day,
    HOUR(t.created_at) AS hour
FROM DEFAULT.twitter_landing_table t
WHERE NOT EXISTS (
    SELECT * FROM twitter_raw_data.place_data_raw tr
    WHERE tr.place_id = t.place_id AND tr.year = YEAR(t.created_at)
        AND tr.month = MONTH(t.created_at) AND tr.day = DAY(t.created_at)
        AND tr.hour = HOUR(t.created_at));



-- Insert the tweet dimensions into the media_data_raw table
INSERT INTO TABLE twitter_raw_data.media_data_raw PARTITION (year, month, day, hour)
SELECT
    --DISTINCT COALESCE(media_media_key, array('No Media')) AS media_id,
    DISTINCT t.media_media_key AS media_id,
    t.media_type AS media_type,
    YEAR(t.created_at) AS year,
    MONTH(t.created_at) AS month,
    DAY(t.created_at) AS day,
    HOUR(t.created_at) AS hour
FROM DEFAULT.twitter_landing_table t
WHERE NOT EXISTS (
    SELECT * FROM twitter_raw_data.media_data_raw tr
    WHERE tr.media_id = t.media_media_key AND tr.year = YEAR(t.created_at)
        AND tr.month = MONTH(t.created_at) AND tr.day = DAY(t.created_at)
        AND tr.hour = HOUR(t.created_at));

