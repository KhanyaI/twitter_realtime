from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
import sqlalchemy
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from urllib3.exceptions import ProtocolError

class Streamlistener(StreamListener):
    
    def __init__(self, api = None, fprefix = 'streamer'):
        self.api = api or API()
        # instantiate a counter
        self.cnt = 0
        # create a engine to the database
        #self.engine = create_engine('sqlite:///app/stream/tweets.sqlite')
        self.engine = create_engine('sqlite:///tweets.sqlite')

    
    def on_status(self, status): 
        # increment the counter
        self.cnt += 1
        # parse the status object into JSON
        status_json = json.dumps(status._json)
        # convert the JSON string into dictionary
        status_data = json.loads(status_json) 
        # initialize a list of potential full-text
        full_text_list = [status_data['text']]
        # add full-text field from all sources into the list
        if 'extended_tweet' in status_data:
            full_text_list.append(status_data['extended_tweet']['full_text'])
        if 'retweeted_status' in status_data and 'extended_tweet' in status_data['retweeted_status']:
            full_text_list.append(status_data['retweeted_status']['extended_tweet']['full_text'])
        if 'quoted_status' in status_data and 'extended_tweet' in status_data['quoted_status']:
            full_text_list.append(status_data['quoted_status']['extended_tweet']['full_text'])
        # only retain the longest candidate
        full_text = max(full_text_list, key=len)
        # extract time and user info
        tweet = {
            'created_at': status_data['created_at'],
            'text':  full_text,
        }
        # uncomment the following to display tweets in the console
        #print("Writing tweet # {} to the database".format(self.cnt))
        #print("Tweet Created at: {}".format(tweet['created_at']))
        #print("Tweets Content:{}".format(tweet['text']))
        #print("User Profile: {}".format(tweet['user']))
        #print()
        # convert into dataframe
        df = pd.DataFrame(tweet, index=[0])
        # convert string of time into date time obejct
        df['created_at'] = pd.to_datetime(df.created_at)
        # push tweet into database
        df.to_sql('tweet', con=self.engine, if_exists='append')
        with self.engine.connect() as con:
            con.execute("""
                        DELETE FROM tweet
                        WHERE created_at IN(
                            SELECT created_at
                                FROM(
                                    SELECT created_at, strftime('%s','now') - strftime('%s',created_at) AS time_passed
                                    FROM tweet
                                    WHERE time_passed >= 60))""")  


if __name__ == '__main__':
    api_key = ""
    api_secret = ""
    access_token_key = ""
    access_token_secret = ""

    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token_key, access_token_secret)
    api =tweepy.API(auth, wait_on_rate_limit=True)
    listener = Streamlistener(api = api)
    myStream = tweepy.Stream(auth, listener = listener)


    #engine = create_engine("sqlite:///tweets.sqlite")

    while True:
        try:
            myStream.filter(track=['digitialhealth','wearables','AI', 'healthtech'],is_async=True)

        except (ProtocolError, AttributeError):
            continue



 