"""API ACCESS KEYS"""

access_token = '1691282078-vkNt0V9aArk7FhAmPOebnmApivsmyBFMk90ob3r'
access_token_secret = 'SQSfHyP9Gv4U1fYctz5YjK0YJGVePyleBVspAsniV2zpn'
consumer_key = 'mwQmK8teqcKDMsSIJETYwE8oc'
consumer_secret = 'Maoqq3xubl8h9CzEIRyDjcH6E8Bo4GJUwuEqTONaCYFmExqmSb'


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


topic_name = "joker"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Joker"], stall_warnings=True, languages= ["en"])


class ListenerTS(StreamListener):

    def on_data(self, data):
            producer.send(topic_name, data.encode('utf-8'))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()