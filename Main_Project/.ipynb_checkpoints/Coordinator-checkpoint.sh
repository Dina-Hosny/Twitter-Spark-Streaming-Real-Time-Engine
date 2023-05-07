#!/bin/bash

# Start the first script in the background
echo "***********************************starting twitter listener***********************************"
python3 ./twitter_listener.py &

# Start the second script in the background
echo "**********************************starting Spark stream**********************************"
spark-submit ./Spark_Streaming.py &

# Wait for 1 minute before starting the third script
echo "**********************************sleeping for 2 min**********************************"
sleep 120

# Run the third script and repeat every 5 minutes
while true; 
do
  # Pause the first two scripts
  
  # Find the process ID of the Spark job
    SPARK_PROCESS_ID=$(jps | grep SparkSubmit | awk '{print $1}')

# Kill the Spark job
    kill "$SPARK_PROCESS_ID"
    
    pkill -f "python3 ./twitter_listener.py"
  
  sleep 20
echo "**********************************starting hive dimensions**********************************"  
  # Run the third script
  spark-sql -f ./HiveDims.hql
  
  
echo "**********************************starting facts**********************************"  

  
  # Run the fourth script
  spark-submit ./facts.py
  
  # Find the process ID of the Spark job
    SPARK_PROCESS_ID=$(jps | grep SparkSubmit | awk '{print $1}')

    # Kill the Spark job
    kill "$SPARK_PROCESS_ID"
    
    python3 ./twitter_listener.py &
    
    spark-submit ./Spark_Streaming.py &
  
  # Wait for 5 minutes
echo "**********************************sleeping for 5 mins**********************************"  

  sleep 300

  
echo "**********************************resume streaming jobs**********************************"  

  # Resume the first two scripts
  python3 ./twitter_listener.py &
  spark-submit ./testSpark.py &
done