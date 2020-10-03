from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, split

if __name__ == "__main__":

    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

    # read the tweet data from socket
    tweet_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "joker")\
        .load()

    # type cast the column value
    tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")


    # split words based on space, filter out hashtag values and group them up
    tweets_tab = tweet_df_string.withColumn('word', explode(split(col('value'), ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False). \
        filter(col('word').contains('#'))

    # write the above data into memory. consider the entire analysis in all iteration (output mode = complete). and let the trigger runs in every 2 secs.
    writeTweet = tweets_tab.writeStream. \
        outputMode("complete"). \
        format("memory"). \
        queryName("tweetquery"). \
        trigger(processingTime='2 seconds'). \
        start()

    print("----- streaming is running -------")