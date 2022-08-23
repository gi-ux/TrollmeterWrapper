from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler
import tweepy
import pandas as pd
import re


class TwitterAuthenticatorV1:
    @staticmethod
    def authenticate_twitter_app(api):
        index = 0
        auth = OAuthHandler(api[0][index], api[1][index])
        auth.set_access_token(api[2][index], api[3][index])
        return auth


class TwitterClientV1:
    def __init__(self, apis, twitter_user=None, tweet_id=None):
        self.auth = TwitterAuthenticatorV1().authenticate_twitter_app(api=apis)
        self.twitter_client = API(self.auth, wait_on_rate_limit=True)
        self.twitter_user = twitter_user
        self.tweet_id = tweet_id

    def search_tweets(self, query, max):
        searched = [status for status in tweepy.Cursor(self.twitter_client.search_tweets, q=query).items(max)]
        if len(searched) > 0:
            json_data = [r._json for r in searched]
            df = pd.json_normalize(json_data)
        else:
            df = pd.DataFrame()
        return df

    def get_active_interactions(self, max_tweets=100):
        query = f"from:{self.twitter_user}"
        df = self.search_tweets(query, max_tweets)
        if ("retweeted_status.created_at" in list(df.columns)) & ("in_reply_to_status_id" in list(df.columns)):
            original = df[df["retweeted_status.created_at"].isna() & df["in_reply_to_status_id"].isna()]
            reply = df[(df["in_reply_to_status_id"].notna()) & (df["retweeted_status.created_at"].isna())]
            retweet = df[df["retweeted_status.created_at"].notna() & (df["in_reply_to_status_id"].isna())]
        elif ("retweeted_status.created_at" in list(df.columns)) & ("in_reply_to_status_id" not in list(df.columns)):
            original = df[df["retweeted_status.created_at"].isna()]
            retweet = df[df["retweeted_status.created_at"].notna()]
            reply = pd.DataFrame()
        elif ("retweeted_status.created_at" not in list(df.columns)) & ("in_reply_to_status_id" in list(df.columns)):
            original = df[df["in_reply_to_status_id"].isna()]
            reply = df[df["in_reply_to_status_id"].notna()]
            retweet = pd.DataFrame()
        else:
            original = df
            retweet = pd.DataFrame()
            reply = pd.DataFrame()
        count = 0
        for i, row in original.iterrows():
            result = re.findall("@([a-zA-Z0-9]{1,15})", row.text)
            if len(result) > 0:
                original.at[i, "mention"] = result[0]
                count += 1
        if count > 0:
            mentions = original[original["mention"].notna()]
            original = original[original["mention"].isna()]
        else:
            mentions = pd.DataFrame()
        return original, reply, retweet, mentions

    def get_passive_rts(self, max_rts=30):
        query = f"RT @{self.twitter_user}"
        return self.search_tweets(query, max_rts)

    def get_passive_rps(self, max_rps=30):
        query = f"to:{self.twitter_user}"
        return self.search_tweets(query, max_rps)

    def get_passive_mentions(self, max_mentions=30):
        query = f"@{self.twitter_user} -filter:retweets -to:{self.twitter_user}"
        return self.search_tweets(query, max_mentions)


