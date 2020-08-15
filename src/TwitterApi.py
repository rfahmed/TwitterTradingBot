# There's a chance that the hardcoded bearer token in setup.py can change.
# In which case we need to code some lines to get the bearer token based on the API keys
# look fat the oauth2 pages on twitter api webpages

import requests
import setup
import os
import json
from twitter import api
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        print(data)
        return True
    def on_error(self, status):
        print(status)


listener = MyStreamListener()
auth = OAuthHandler(setup.twitterApiKey, setup.twitterSecretKey)
auth.set_access_token(setup.twitterAccessToken, setup.twitterAccessSecret)
stream = Stream(auth, listener)
stream.filter(track=["Walgreens -is:retweet"])

def search():
    #authorization for the twitterapi endpoint
    headers = {
        "Authorization": "Bearer " + setup.twitterBearerToken
    }
    #these are the parameters for search/stream
    payload = {
        "query" : "Walgreens -is:retweet"
    }
    r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=payload, headers=headers)
    print(r.json())

def stream():
    # authorization for the twitterapi endpoint
    headers = {
        "Authorization": "Bearer " + setup.twitterBearerToken
    }
    #This creates a filtered stream based on the Key word Walgreens
    data = {
        "add": [{"value": "Walgreens -is:retweet", "tag": "Walgreen tweets"}]
    }
    param = {
        "tweet.fields": "created_at"
    }
    tempheader = {
        "Content-type": "application/json",
        "Authorization": "Bearer " + setup.twitterBearerToken
    }
    # requests.get("https://api.twitter.com/1.1/application/rate_limit_status.json?resources=help,users,search,statuses", data=data, headers=headers)
    r = requests.get("https://api.twitter.com/2/tweets/search/stream", headers=headers)
    requests.post("https://api.twitter.com/2/tweets/search/stream/rules", stream=True, data=data, headers=tempheader)
    x = requests.get("https://api.twitter.com/2/tweets/search/stream/rules", stream=True, headers=headers)
    print("test")
    print(x.json())



