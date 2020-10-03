# uses python3
from __future__ import print_function
import json
#from kafka import KafkaProducer, KafkaClient
from kafka import KafkaProducer, KafkaClient
import tweepy

# Twitter Credentials Obtained from http://dev.twitter.com
consumer_key = 'mwQmK8teqcKDMsSIJETYwE8oc'
consumer_secret = 'Maoqq3xubl8h9CzEIRyDjcH6E8Bo4GJUwuEqTONaCYFmExqmSb'
access_key = '1691282078-vkNt0V9aArk7FhAmPOebnmApivsmyBFMk90ob3r'
access_secret = 'SQSfHyP9Gv4U1fYctz5YjK0YJGVePyleBVspAsniV2zpn'

# Words to track
WORDS = ['joker']

class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print("Error received in kafka producer " + repr(status_code))
        return True # Don't kill the stream

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        try:
            producer.send('joker', data.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

# Kafka Configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Create Auth object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=60, retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
stream = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
stream.filter(track=WORDS, languages = ['en'])