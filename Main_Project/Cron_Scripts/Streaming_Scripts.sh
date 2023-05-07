#!/bin/bash

# Start the first script in the background
echo "***********************************starting twitter listener***********************************"
python3 ./twitter_listener.py &

# Start the second script in the background
echo "**********************************starting Spark stream**********************************"
spark-submit ./Spark_Streaming.py &
