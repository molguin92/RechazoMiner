import json

import tweepy as tp

with open('./auth.json', 'r') as f:
    keys = json.load(f)

auth = tp.AppAuthHandler(keys['api_key'], keys['api_secret'])
api = tp.API(auth)
for tweet in tp.Cursor(api.search, q='tweepy').items(10):
    print(tweet.text)
