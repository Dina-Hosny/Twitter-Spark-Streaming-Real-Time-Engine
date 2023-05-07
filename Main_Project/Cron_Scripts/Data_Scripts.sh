#!/bin/bash


echo "**********************************starting hive dimensions**********************************"  

  spark-sql -f ./HiveDims.hql
  
echo "**********************************sleeping for 10 sec**********************************"  

  sleep 10

   
echo "**********************************starting facts**********************************"  

  
  spark-submit ./facts.py

echo "**********************************sleeping for 10 sec**********************************"  

  sleep 10