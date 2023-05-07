#!/bin/bash

# Start twitter_listener.py and Spark_Streaming_job.py in the background
python3 ./twitter_listener.py &
spark-submit ./testSpark.py  &

# Run HiveDimsTry.sql every 40 seconds
while true
do
    spark-sql -f ./HiveDimsTry.sql
    sleep 40
done &

# Wait for 180 seconds before killing the background processes
sleep 180
pkill -f "python3 ./twitter_listener.py"
pkill -f "spark-submit ./testSpark.py"
pkill -f "spark-sql -f ./HiveDimsTry.sql"


#python3 ./twitter_listener.py & spark-submit ./Spark_Streaming_job.py

#sleep 40 

#spark-sql -f ./HiveDimsTry.sql

#sleep 10


#kill after 3 mins


#sleep 180

#pkill -f "python3 ./twitter_listener.py"
#pkill -f "spark-submit ./Spark_Streaming_job.py"
#pkill -f "spark-sql -f ./HiveDimsTry.sql"