from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient
import json
import re
import tweepy
from datetime import datetime

TWEET_TOPICS = ['asos']

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'twitter_events'

class Streamer(StreamListener):

    def on_error(self, status_code):
        if status_code == 402:
            return False

    def on_status(self, status):
        if(not status.text.startswith(('@', 'RT@'))):
            producer.send(KAFKA_TOPIC, value=status._json)
            d = datetime.now()
            print(f'[{d.hour}:{d.minute}.{d.second}] sending tweet')

# put your API keys here
consumer_key = ""
consumer_secret_key = ""

access_token = "" 
access_token_secret = ""

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda x: json.dumps(x).encode('utf-8'))    
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')
    sys.exit(1)

l = Streamer()
auth = OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=TWEET_TOPICS, languages=['en'])