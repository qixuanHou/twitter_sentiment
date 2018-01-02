import json
import re
import boto.dynamodb
from boto.dynamodb2.table import Table
import time
import storm
import ast
from datetime import date
import datetime
import time
from decimal import Decimal
from awscredentials import AWS_KEY,AWS_SECRET, REGION
import boto3
from decimal import Decimal
from nltk.sentiment.vader import SentimentIntensityAnalyzer


conn_db = boto3.resource('dynamodb', aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET,
                            region_name=REGION)


# sentimentIntenstiy
sia = SentimentIntensityAnalyzer()


def real_time_batch_processing(tweet):
    tweets = {}
    mood_level = sia.polarity_scores(tweet["text"])
    tweet_map = {"timestamp": tweet["timestamp"],
        "screen_name": tweet["screen_name"],
        "user_id": tweet["user_id"],
        "mood": Decimal(str(mood_level["compound"])),
        "text": tweet["text"],
        "pos": Decimal(str(mood_level["pos"])),
        "neg": Decimal(str(mood_level["neg"])),
        "neu": Decimal(str(mood_level["neu"]))}
    return tweet_map

class SensorBolt(storm.BasicBolt):
    def process(self, tup):
        data = tup.values[0]
        tweet = ast.literal_eval(data)
        output = real_time_batch_processing(tweet)
        table = conn_db.Table("realtime_db")
        table.put_item(Item=output)
        storm.emit([output])

SensorBolt().run()
