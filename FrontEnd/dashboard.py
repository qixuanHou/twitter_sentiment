import flask
#import findspark
import os
from flask import request, render_template, jsonify
import boto3
from boto3.dynamodb.conditions import Key, Attr
import datetime
import time
import json
import sys
from flask import request
from datetime import timedelta
import tweepy
from tweepy import OAuthHandler
from awscredentials import AWS_KEY, AWS_SECRET, REGION
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
import ast
import pyspark
from decimal import Decimal
from operator import itemgetter
from nltk.tokenize import word_tokenize
import pickle
reload(sys)  
sys.setdefaultencoding('Cp1252')
#findspark.init()
#twitter credentials


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

topAccountsList = []
fh = open("topAccounts.txt", "r")
for line in fh:
    topAccountsList.append(line.rstrip("\r\n"))
fh.close()


#file directory
dir_template = os.path.dirname(os.path.realpath(__file__))
print (dir_template)

# Create the application.
APP = flask.Flask(__name__)

# spark
# spark initialzation
conf = pyspark.SparkConf().setAppName('sentiment-analysis').setMaster('local[*]')
sc = pyspark.SparkContext(conf=conf)

# sentimentIntenstiy
sia = SentimentIntensityAnalyzer()

dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET,
                            region_name=REGION)

@APP.route('/',  methods=['GET', 'POST'])
def index():
    return render_template('index.html')

def real_time_get_timeline(user_id):
    allTweets = {}
    single_col = []
    timeline = api.user_timeline(screen_name = user_id, count = 100)
    realTweets = []
    for status in timeline:
        json_form = status._json
        source_type = "not known"
        source = json_form["source"].encode('utf8')
        tweet = json_form["text"].encode('utf8')
        tweet_raw = json_form["text"].encode('utf8')
        tweet_arr = tweet.split(" ")
        interacted = "NULL"
        used_tag = "NULL"
        # user_id = json_form["id"].encode('utf8')
        time = json_form["created_at"].encode('utf8')
        time_arr = time.split(" ")
        month = ""
        if time_arr[1] == "Jan":
            month = "01"
        elif time_arr[1] == "Feb":
            month = "02"
        elif time_arr[1] == "Mar":
            month = "03"
        elif time_arr[1] == "Apr":
            month = "04"
        elif time_arr[1] == "May":
            month = "05"
        elif time_arr[1] == "Jun":
            month = "06"
        elif time_arr[1] == "Jul":
            month = "07"
        elif time_arr[1] == "Aug":
            month = "08"
        elif time_arr[1] == "Sep":
            month = "09"
        elif time_arr[1] == "Oct":
            month = "10"
        elif time_arr[1] == "Nov":
            month = "11"
        elif time_arr[1] == "Dec":
            month = "12"
        time = "%s-%s-%s %s" % (time_arr[5], month, time_arr[2], time_arr[3])
        interacted = []
        used_tag = []
        pure_name = ""
        for text in tweet_arr:
            if "://" in text:
                tweet = tweet.replace(text, '')
            if "@" in text:
                pure_name = re.sub("[^A-Za-z]",'',text.strip())
                if pure_name.lower() != user_id.lower():
                    interacted.append(pure_name)
            if "#" in text:
                used_tag.append(re.sub("[^A-Za-z]",'',text.strip()))
        tweet = re.sub("[^A-Za-z]"," ", tweet.strip())
        tweet = ' '.join(tweet.split())
        retweeted = "false"
        if tweet[:3] == "RT ":
            retweeted = "true"
        if "iphone" in source:
            source_type = "Iphone"
        elif "web" in source:
            source_type = "Web"
        elif "ipad" in source:
            source_type = "Ipad"
        elif "media" in source:
            soruce_type = "media studio"
        else:
            source_type = "Possibly android"
        if len(tweet) < 3:
            continue
        if len(interacted) == 0:
            interacted = "NULL"
        if len(used_tag) == 0:
            used_tag = "NULL"
        strCon = {"time": time, "text_raw": tweet_raw, "text": tweet, "fav_count": str(status.favorite_count),
        "source_type": source_type, "is_retweeted": retweeted, "interacted": interacted, "hashtag": used_tag}
        realTweets.append(strCon)
    return realTweets

def real_time_batch_processing(realTweets):
    tweets = sc.parallelize(realTweets)
    tweet_map = tweets.map(lambda s: {"time": s["time"],
        "mood": sia.polarity_scores(s["text"]).get("compound"), "text": s["text_raw"],
        "pos": sia.polarity_scores(s["text"]).get("pos"),
        "neg": sia.polarity_scores(s["text"]).get("neg"),
        "neu": sia.polarity_scores(s["text"]).get("neu")}).collect() # list
    return tweet_map

#user page where we show everything
@APP.route('/user/<userAccount>',  methods=['GET', 'POST'])
def get_profile(userAccount):
    userInfo = get_basic_info(userAccount)
    output = []
    output.append(userInfo)

    #UserID is numerical ID for Twitter / this data works as a key
    UserID = userInfo["ScreenName"]

    #sentimentData = get_sentiment(UserID)
    # sentimentData = []
    # streamData = get_stream(UserID)
    streamData = [0]
    real_time_data = real_time_get_timeline(UserID)
    tweet_mapped = real_time_batch_processing(real_time_data)
    boolArr = [True, False, True, False, True, False, True, False, True, False]
    top_10_most_tweets = sorted(tweet_mapped, key=itemgetter("time"), reverse=True)
    ts = time.time()
    X = []
    Y = []
    tweets = []
    timeDiff = []
    tweets_plot = []
    highest = 0
    lowest = 100
    highest_tweet = ""
    lowest_tweet = ""
    for i in range(len(top_10_most_tweets)):
        x = tweet_mapped[i]["time"]
        y = tweet_mapped[i]["mood"]
        X.append(x)
        Y.append((float(y) + 1) * 50)
        tweets.append(tweet_mapped[i]["text"].decode("utf-8"))
        text = tweet_mapped[i]["text"].decode("utf-8").encode('ascii', 'ignore')
        tweets_plot.append(text[0:20] + "...")
        nextTS = time.mktime(datetime.datetime.strptime(tweet_mapped[i]["time"], "%Y-%m-%d %H:%M:%S").timetuple())
        current_seconds = ts - nextTS
        hour = current_seconds / 60 / 60

        timeDiff.append(int(round(hour)))
        if x > highest:
            highest = y
            highest_tweet = text
        if x < lowest:
            lowest = y
            lowest_tweet = text
    print("highest: " + str(highest) + " tweet: " + str(highest_tweet))
    return render_template('result.html', output = output, UserID = userAccount, streamData = streamData,
        topten = tweets[0:10], tweets=tweets_plot, boolArr=boolArr, X=json.dumps(X), Y=Y, hours = timeDiff,
        highest = highest, highest_tweet = highest_tweet, lowest = lowest, lowest_tweet = lowest_tweet)

#basic info returns User's name, screen name, picture URL and User ID
def get_basic_info(userAccount):
    users = api.search_users(userAccount,20,0)
    # TODO if no users found return something
    followers = 0
    User = users[0]
    for person in users:
        if person.followers_count > followers:
            User = person
            followers = person.followers_count
    UserName = User.name
    ScreenName = User.screen_name
    # TODO if user has less than 100 status return somethin
    generalUrl = "https://twitter.com/NAME/profile_image?size=original"
    picUrl = generalUrl.replace("NAME", ScreenName)
    basicInfo = {"UserName" : UserName, "ScreenName" : ScreenName, "picUrl" :picUrl}
    return basicInfo

@APP.route('/get_public_tweet/<userAccount>', methods=["GET"])
def get_public_tweet(userAccount):
    """
    if userAccount in topAccountsList:
        return jsonify(0)
    return jsonify([{"text": "Test data1", "name": "testJack1"}, {"text": "Test data2", "name": "testJack2"}, {"text": "Test data3", "name": "testJack3"}, {"text": "Test data4", "name": "testJack4"}, {"text": "Test data5", "name": "testJack5"}])
    """
    results = get_stream(userAccount)
    #for i in results:
    #   i["mood"] = NaiveBayesClassifier(i["tweet"])
    return jsonify(results)


@APP.route('/get_publicopinion/<userAccount>')
def get_publicopinion(userAccount):
    """
    if userAccount in topAccountsList:
        return get_stream(userAccount)
    return jsonify(0)
    """
    results = get_stream(userAccount)
    size = len(results)
    number = 0
    for i in range(0, size):
        number += (results[i]["mood"] +1) * 50
    if size == 0:
        size = 1
    return jsonify(number/size)

def NaiveBayesClassifier(sentence):
    train = []
    def addTrainingData(fileName): 
        with open(fileName) as f:
            for line in f:
                char = line[-1:]
                sentence = str.split(line,"\t")[0]
                if(char == "1"):
                    train.append((sentence,"pos"))
                else:
                    train.append((sentence,"neg"))

    addTrainingData("amazon_cells_labelled.txt")
    addTrainingData("imdb_labelled.txt")
    addTrainingData("yelp_labelled.txt")
    dictionary = set(word.lower() for passage in train for word in word_tokenize(passage[0]))

    f = open('my_classifier.pickle', 'rb')
    classifier = pickle.load(f)
    f.close()

    sentence_features = {word.lower(): (word in word_tokenize(sentence.lower())) for word in dictionary}
    sentiment = str(classifier.classify(sentence_features))
    return sentiment

def get_stream(UserID):

    table = dynamodb.Table('realtime_db')
    #timestamps
    ts=time.time()
    nowTime = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    #intervolTime = datetime.datetime.fromtimestamp(ts-3600).strftime('%Y-%m-%d %H:%M:%S')
    # Time window between

    # for test
    intervolTime = "2016-12-06 07:33:07"

    ke = Key("user_id").eq(UserID)
    fe = Key("timestamp").between(intervolTime , nowTime)

    response = table.query( KeyConditionExpression=ke & fe, ScanIndexForward = False)
    DBdata = response["Items"]
    size = len(DBdata)
    number = 0
    results = []
    dictA = {}
    for i in range(0,size):

        ID = DBdata[i]["user_id"]
        mood = DBdata[i]["mood"] 
        timestamp = DBdata[i]["timestamp"]
        #timeDiff = timestamp - nowTime
        tweet = DBdata[i]["text"]
        senderName = DBdata[i]["screen_name"]
        dictA = {"ID" : ID, "mood" : mood, "timestamp" : timestamp, "tweet" : tweet , "mood" : mood, "senderName" : senderName, "size" : size}
        results.append(dictA)

    return results

if __name__ == '__main__':
    APP.debug=True
    APP.run(host='0.0.0.0', port=5000)
