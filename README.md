
# Twitter Streaming Real-Time Engine

An Apache Spark streaming project that aims to collect, process, and analyze Twitter data in real-time with dashboard insights using various tools and frameworks like Python3, PySpark, Hive, and Bokeh.

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

The Data Source System is the initial stage of the project pipeline. The system collects tweets, user information, and location data from Twitter's APIv2 by specifying relevant keywords.

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



