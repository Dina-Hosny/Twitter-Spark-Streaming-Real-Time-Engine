# For sending GET requests from the API
import requests

# For saving access tokens and for file management when creating and adding to the dataset
import os
import sys


# For dealing with json responses we receive from the API
import json

# For displaying the data after
import pandas as pd

# For saving the response data in CSV format
import csv

# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
import time

from datetime import date
from datetime import timedelta


#To add wait time between requests
import time

#To open up a port to forward tweets
import socket 

# for operations
import re

import traceback


os.environ['TOKEN'] = "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, next_token=None, max_results = 100):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" 
    


    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    #'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'expansions': 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id,edit_history_tweet_ids',
                    'tweet.fields': 'attachments,id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    #'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,edit_history_tweet_ids,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,promoted_metrics,public_metrics,referenced_tweets,reply_settings,source,text',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,location,pinned_tweet_id,profile_image_url,protected,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    
    return (search_url, query_params)
    
    

def get_response(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    print(f"Endpoint Response Code: {str(response.status_code)}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_tweet_data(next_token = None, query='ramadan', max_results=100):
    

    # Inputs for the request
    
    
    start_date = str(date.today() - timedelta(days=6))
    end_date = str(date.today())
    #end_time = f"{end_date}T00:00:00.000Z"
    
    bearer_token = auth()
    headers = create_headers(bearer_token)
    keyword = f"{query} lang:en has:hashtags"
    #start_time = "2023-04-16T00:28:00.000Z"
    start_time = f"{start_date}T00:00:00.000Z"
    #end_time = "2023-04-22T00:00:00.000Z"
    end_time = f"{end_date}T00:00:00.000Z"
    

    url: tuple = create_url(keyword, start_time,end_time, next_token=next_token, max_results = 100)
    json_response = get_response(url=url[0], headers=headers, params=url[1])

    return json_response


def get_tag(tweet):
    tags = re.findall(r'\B#\w*[a-zA-Z]+\w*', tweet)
    #print(tweet)
    #print(tweet.encode('utf-8'))
    #print(f"Tags: {tags}")
    return tags



def send_tweets_to_spark(http_resp, tcp_connection, batch_id):
    data = http_resp["data"]
    includes = http_resp.get("includes", [])
    
    print(list(includes.keys()))
    print (list(http_resp.keys()))

    users = {}
    media_dict = {}
    places_dict = {}
    


    for user in includes["users"]:
        users[user["id"]] = user

    for media in includes["media"]:
        media_dict[media["media_key"]] = media
        
    for place in includes.get("places", []):
        places_dict[place["id"]] = place

    for tweet in data:
        
        tweet['batch_id'] = batch_id # add index number to tweet
        #batch_id += 1 # increment index for next tweet
        
        if "author_id" in tweet:
            user = users.get(tweet["author_id"], {})
            tweet["user"] = user

        if "attachments" in tweet and "media_keys" in tweet["attachments"]:
            tweet_media = [media_dict[m_key] for m_key in tweet["attachments"]["media_keys"]]
            tweet["media"] = tweet_media[0] if tweet_media else {}
            
        if "geo" in tweet and "place_id" in tweet["geo"]:
            place_id = tweet["geo"]["place_id"]
            place = places_dict.get(place_id, {})
            tweet["place"] = place
        
        tweet['hashtags'] = get_tag(tweet["text"])
           

        try:
            tweet_str = json.dumps(tweet)
            tcp_connection.send((tweet_str + '\n').encode("utf-8"))
        except BrokenPipeError:
            exit("Pipe Broken, Exiting...")
        except KeyboardInterrupt:
            exit("Keyboard Interrupt, Exiting...")
        except Exception as e:
            traceback.print_exc()
       
            
if __name__ == '__main__':
    no_of_pages = 2
    queries = ['Technology','Sport','Fun','Education','Learn']
    max_results = 100
    sleep_timer = 5
    
    TCP_IP = "127.0.0.1"
    TCP_PORT = 7777
    
    stop_signal = "STOP_LOOP"
    
    batch_id = 1 # initialize index variable
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    print("Listening on port: %s" % str(TCP_PORT))
    s.listen(1)
    print("Waiting for the TCP connection...")       
    
    conn, addr = s.accept()
    
    print("Connected successfully... Starting getting tweets.")
    
    next_token = None
    

    
    while True:
        for query in queries:
            try:
                print(f"\n\n\t\tProcessing for keyword {query}\n\n")
                resp = get_tweet_data(next_token=next_token, query=query, max_results=max_results)

                next_token = resp['meta']['next_token']
                send_tweets_to_spark(http_resp=resp, tcp_connection=conn, batch_id = batch_id)
                batch_id += 1 # increment index for next batch
                #time.sleep(sleep_timer*60)
            except KeyboardInterrupt:
                exit("Keyboard Interrupt, Exiting..")
                #tcp_connection.send(stop_signal.encode('utf-8'))
                sys.exit(0)
                conn.sendall(b'error')
                s.close()
                print('Socket Closed')

                break
                
            except Exception as e:
                #tcp_connection.send(stop_signal.encode('utf-8'))
                print("Error occurred: ", e)
                conn.sendall(b'error')
                pass
        print("\n \n Waiting 5 mins to push the next batch")
        time.sleep(sleep_timer*60)
            

    s.close()
    