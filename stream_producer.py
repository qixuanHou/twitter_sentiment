from random import randrange
import time
import datetime
from kafka.client import KafkaClient
from kafka import KafkaProducer
import json
from awscredentials import AWS_EC2_DNS
import tweepy
from tweet_config import consumer_key, consumer_secret, access_token, access_token_secret
import boto3


topAccounts = []
fh = open("topAccounts.txt", "r+")
for line in fh:
    topAccounts.append(line.strip("\r\n"))
fh.close()
producer = KafkaProducer(bootstrap_servers="ip:6667")



def publish(tweet):
    ts=time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    isAccountFound = False
    data = {}
    data["timestamp"] = str(timestamp)
    data["text"] = tweet.text
    text = tweet.text
    for account in topAccounts:
        if account in text:
            data["user_id"] = account
            isAccountFound = True
    data["screen_name"] =  str(tweet.user.screen_name)
    if isAccountFound:
        producer.send('forestfire', value=json.dumps(data).encode("utf-8"))
    time.sleep(3)
    # return data


#lambda function
class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        publish(status)
        # put_data(self.data1)
    def on_error(self, status_code):
        print(status_code)

if __name__ == "__main__":
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    streamListener = StreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=streamListener)
    myStream.filter(track=topAccounts, languages=["en"])