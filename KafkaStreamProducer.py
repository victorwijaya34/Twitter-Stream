"""API ACCESS KEYS"""

access_token = ''
access_token_secret = ''
consumer_key = ''
consumer_secret = ''


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
