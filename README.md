
# Twitter Spark Streaming Real-Time Engine

An Apache Spark streaming project that aims to collect, process, and analyze Twitter data in real-time with dashboard insights using various tools and frameworks like Python, Spark, HDFS, Hive, and Bokeh.

## Project Description

This project uses a Real-Time Analytics Engine which is Apache Spark streaming to process tweets retrieved from Twitter API based on certain keywords, storing it in the Hadoop Distributed File System and Hive tables, Processing it, and finally representing some extracted insights data in a real-time dashboard using Bokeh web framework.

Overall, the project enables the efficient collection, processing, and analysis of Twitter data, with the resulting insights presented in a clear and user-friendly format.

## Architecture Overview

![New Project (1) (1)](https://user-images.githubusercontent.com/46838441/236661118-abc7c938-da95-4015-84ed-9b7d3fcbbb22.png)

The project involves six data pipelines that work together to collect and process live Twitter data. The data is first streamed through a TCP socket using a Python Twitter listener and fed into a Spark processing engine. From there, the processed data is stored in HDFS parquet format.

The stored data is then read into a Star Schema model that includes several Hive Dimension tables. Using a SparkSQL application, these dimensions are analyzed and used to create Hive Fact Tables. These Fact Tables provide the foundation for creating analytical insights and visualizations.

## Project Workflow

#### a detailed overview of how different project pipelines work

<p align="center">
  <img src="https://user-images.githubusercontent.com/46838441/236661208-7bf0dbd3-18cd-49a1-82da-37e25fa6c4cf.png">
</p>


**1- Data Source System:**

The Data Source System is the initial stage of the project pipeline. The system collects tweets, user information, and location data from Twitter's [APIv2](https://developer.twitter.com/en/docs/twitter-api/getting-started/make-your-first-request) by specifying relevant keywords.

This process is executed by a Python listener script that fetches the latest tweets, along with the associated author and location data. In addition, media information and hashtags are also extracted from each tweet. The script is set to execute every five minutes, ensuring that the data collected is up-to-date and relevant.

Once the data is extracted, it is pushed to an arbitrary port, which acts as a TCP socket to enable communication with the next stage of the pipeline. This ensures that the collected data is readily available for processing and analysis.

**2- Data Collection System:**

The Data Collection System is responsible for collecting, processing, and storing Twitter data.

This stage involves a long-running job that acts as the data collector from the port, serving as a link between the Twitter API and the Hadoop Distributed File System (HDFS). The Twitter data is sent from the Python Twitter Listener in a JSON format through a TCP Socket to the Spark Streaming Job application.

Using PySpark, the data is then processed and cleaned before being parsed into a PySpark data frame with a well-defined schema. The data is stored in HDFS in a parquet format and partitioned by the Year, Month, Day, and Hour, extracted from the *'created_at'* column that represents the creation date of the tweet.

The Data Collection System runs continuously, keeping the stream up and running and receiving data from the port that was opened in the previous stage of the pipeline. This ensures that the data collected is stored efficiently and can be used for further processing and analysis.

**3- Landing Data Persistence:**

The Landing Data Persistence stage involves the creation of a Hive table, `"twitter_landing_data"`, that is built on top of the `"twitter-landing-data"` directory where the data parquet files are stored.

This Hive table serves as a metadata layer that provides a logical representation of the stored data. It enables users to query the data using SQL-like syntax, facilitating easy data access and analysis.

**4- Landing to Raw ETL:**

The Landing to Raw ETL stage involves the creation of necessary Hive dimension tables using HiveQL queries. These dimensions are extracted from the landing data, specifically from the `"twitter_landing_data"` table that was created in the previous stage of the pipeline.

This stage creates four Hive dimensions, namely "tweet_data_raw", `"user_data_raw"`, `"place_data_raw"`, and `"media_data_raw"`, containing tweets' info, users' info, locations' info, and tweet attachments' media info, respectively. All output dimensions are stored in an HDFS directory called `"twitter_raw_data"` and partitioned by the same four columns used in the previous stage.

**5- Raw To Processed ETL:**

In this stage, the raw data from the previous stage is transformed into processed data by reading the Hive dimensions into a SparkSQL application and applying various aggregates to create Hive fact tables. The fact tables provide the basis for business insights and analysis. 

The created Hive fact tables include `"tweet_engagement_processed"`, which contains metrics related to tweet engagement, `"user_activity_processed"`, which contains majors that describe users' activities, `"users_tweets_processed"`, which contains majors related to the tweet and its authors, and `"totals_metrics_processed"`, which contains majors that calculate totals required for various analysis processes. Finally, all the output facts are stored in an HDFS directory named `"twitter-processed-data"`.

**6- Real-time Data Visualization:**

This is the final stage of the project pipeline. It involves presenting the extracted insights data in a dynamic dashboard using Bokeh, which is a Python-based data visualization library. The dashboard provides interactive data exploration and visualization tools that enable users to gain insights into trending topics, user sentiment, and other key metrics.

The extracted insights include:
- "Top 10 hashtags by frequency of occurrence"
- "Top 10 popular users by number of followers"
- "Number of tweets per day of the week"
- "Distribution of media types by frequency of occurrence"
- "Count of high, medium, and low popularity users"

**Shell Script Coordinator:**

To manage the project pipelines, a Shell script coordinator is utilized. The coordinator contains two Shell scripts to handle the different pipeline processes.

The first Shell script runs once during machine startup and continues running in the background to ensure the streaming process is always active. It includes the Python Listener and Spark Streaming Job scripts.

The second Bash script comprises the Hive Dimensions script, the Hive Facts, and the Dashboard script. This script is added to the Linux Cron Jobs file and runs every five minutes, ensuring that the data in the HDFS, Hive tables, and Dashboards are up-to-date.

## Graphical View of Hive Tables

<p align="center">
  <img src="https://user-images.githubusercontent.com/46838441/236668826-13da1285-f709-45a0-9549-2bf016eb87db.png">
</p>

**Hive Dimensions Tables**

- ***1- tweet_data_raw:***
This Hive dimension table contains information related to tweets, including `tweet ID`, `author ID`, `tweet text`, `hashtags list`, `retweet count`, `reply count`, `like count`, `quote count`, `impression count`, `language`, `tweet date`, `who can reply` which represents the reply applied settings on the tweet, `media IDs`, `tweet location`, `edit history IDs`.

The partition information columns such as `year`, `month`, `day`, and `hour` are used for efficient querying and data retrieval. 

This table can be used to analyze and gain insights into Twitter user behavior and engagement.

- ***2- user_data_raw:***
This Hive dimension table contains information about Twitter users, such as their `user ID`, `username`, `name`, `profile description`, `location`, `profile image`, and other related metadata.

It also includes various statistics about their account, such as `follower count`, `following count`, `tweet count`, and `listed count`.

Additionally, there is a column for `user popularity`, which is calculated based on their social media activity. so if the user has more than 5K followers the popularity will be 'High' and if he has between 2K and 5K it will be 'Middle' and if he has less than 2K it'll be 'Low'.

The table is partitioned by `year`, `month`, `day`, and `hour`.

- ***3- place_data_raw:***

This Hive dimension table that contains information about the locations of the tweets. The columns include the `place ID`, `name`, `full name`, `country`, `country code`, and `type`. 

The table is partitioned by `year`, `month`, `day`, and `hour`.

- ***4- media_data_raw:***

This Hive dimension table contains information about the media uploaded in the tweets. The columns include `media_id` and `media_type`.

The table is partitioned by `year`, `month`, `day`, and `hour`.

**Hive Fact Tables**

- ***1- tweet_engagement_processed:***
this Hive fact table contains processed engagement data for each tweet. It includes columns such as `tweet_id`, and columns for the number of `likes`, `retweets`, `replies`, `quotes`, and `impressions` the tweet received. Additionally, the table includes columns for the count of `hashtags` and `media` that were included in each tweet.

This table is likely used to analyze tweet engagement and popularity over time.

- ***2- user_activity_processed:***
This Hive fact table contains information about the activity of users on Twitter, including the number of tweets, followers, and accounts they are following. The table has four columns: `user_id`, `tweet_count`, `followers_count`, `following_count`.

- ***3- users_tweets_processed:***

This hive fact table contains data related to tweets and their respective users. It has 4 columns, including `tweet_id`, `user_id`, `num_hashtags` indicating the number of hashtags that the user wrote, and `num_media` indicating the number of media files tweeted by the user.

- ***4- totals_mertics_processed:***

This Hive fact table contains processed metrics for `tweets`, `users`, `places`, and `media`. It represents some totals that may be needed in the different analyses.

The table includes counts for various metrics such as `weekend` and `weekday` tweet counts, `verified` and `protected` user counts, and counts for `high`, `middle`, and `low` popularity users. Additionally, the table includes counts for `photo` and `video` media, the number of distinct `cities` in which the tweet was posted, and a count of tweets with `limited replies` which have non-public reply settings.

## Project Files:

*The project includes 3 directories:*

***1- Main_Project:*** 
which houses the main project files.

- `Twitter_Listener.py`: A Python script with functions that use Twitter API2 to retrieve streaming data from Twitter. The data is in JSON format and is sent via a TCP socket to the next stage. The data comprises tweets, users, places, and media information, as well as a list of hashtags extracted from each tweet and a batch ID used to identify each batch of data transferred.

- `Spark_Streaming.py`: The spark job which responsible for collecting, cleaning, and processing data from the Twitter_Listener. The data is then loaded into the HDFS in a partitioned parquet format using Year, Month, Day, and Hour columns as partitions. This Spark job serves as an ETL engine between the listener and the HDFS. Lastly, the Spark job creates a Hive table containing all the raw parquet data.

- `HiveDims.hql`: A Hive script with queries used for creating and inserting data into the Hive Dimensions tables.

- `facts.py`: a SparkSQL script that uses Hive dimensions tables to create facts tables with meaningful metrics and generate business insights from the collected data. The script also contains the Bokeh code to create a real-time dashboard for visualizing the data.

- `Coordinator.sh`: A Shell script that automates the execution of all pipeline scripts. It ensures that the Python listener and the Spark Streaming Job are always running by sending them to the background. The HiveDims and Fact scripts are run every 5 minutes.

    _It is important to note that the streaming scripts are stopped before running other scripts and restarted after they finish, as the machine cannot handle more than one Spark job at a time. However, this script is intended for testing purposes only and may not provide optimal performance._

- `Dashboard`: includes the `twitter_charts.html` file that displays real-time charts created using Bokeh.

- `Requirements.txt`: a file that lists all the necessary packages and libraries needed to run the project.

- `Cron_Scripts`: This directory contains Shell scripts that automate the entire pipeline using Linux Crontab. the `Crontab_Command.txt` file contains commands to run the `Data_Scripts.sh` every 5 minutes, which executes the HiveDims and Facts processes. Additionally, the directory includes `Streaming_Scripts.sh`, which runs the streaming jobs in the background, ensuring they remain up and running.

***2- Test_Files:*** 
This directory includes files and Jupyter Notebooks that were used for testing different parts of the project and verifying their outputs.

## Running the Project

**Pre-requests:**

- 1- Make sure that the Java version is compatible with Pyspark.
- 2- Clone the repository

```bash
  git clone https://github.com/Dina-Hosny/Twitter-Spark-Streaming-Real-Time-Engine.git
```

- 3- Navigate to the project directory

```bash
  cd Main_Project
```

- 4- install the needed packages and librariers

```bash
  pip install -r ./Requirements.txt
```

**Launch the project:**

- 1- copy and paste commands into `Crontab_Command.txt` file to your crontab file system.
- 2- run `Streaming_Scripts.sh` file
```bash
  sh Streaming_Scripts.sh
```

## Limitations
- Project can only streams 450 queries per 15 minutes [enforced by Twitter APIv2](https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent) .
- Project can only streams 500K queries per month [enforced by twitter APIv2](https://developer.twitter.com/en/docs/twitter-api/rate-limits) .
- Project can't get general tweets from Twitter. It can get tweets based on some keywords enforced by Twitter APIv2.
- Project can get tweets which created in the last 7 days only enforced by Twitter APIv2.
- The maximum result can be retrived with each batch is 100 enforced by Twitter APIv2.

## Project Demo

https://user-images.githubusercontent.com/46838441/236680536-401e6b56-4e79-4dc4-a339-5cec5e0b0c78.mp4

## Output Real-Time Dashboard

https://user-images.githubusercontent.com/46838441/236680693-1e77e456-eaf5-4063-8ee0-c20652d30cbd.mp4

![1](https://user-images.githubusercontent.com/46838441/236680572-9dc5f4e5-aa21-4e04-98c9-b0d8e0bb3e5b.png)

![2](https://user-images.githubusercontent.com/46838441/236680583-a384b8d8-9b58-4076-a1f0-375e5a3a2c4f.png)

![3](https://user-images.githubusercontent.com/46838441/236680587-cf223eef-c3e2-4558-9d5b-46f7be676dc2.png)

![4](https://user-images.githubusercontent.com/46838441/236680591-2b4883f0-7058-4a1c-b5ba-bc7778c4762f.png)

![5](https://user-images.githubusercontent.com/46838441/236680595-73223719-149f-4829-9a8b-17ad26a8ff9d.png)


