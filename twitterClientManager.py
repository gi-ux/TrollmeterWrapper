from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler
import tweepy


class TwitterClientV1:
    def __init__(self, apis, twitter_user=None, tweet_id=None):
        self.auth = TwitterAuthenticatorV1().authenticate_twitter_app(api=apis)
        self.twitter_client = API(self.auth, wait_on_rate_limit=True)
        self.twitter_user = twitter_user
        self.tweet_id = tweet_id

    def get_user_timeline_tweets(self, num_tweets=100, repbool=False, rtsbool=True):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user, tweet_mode='extended',
                            include_entities=True, exclude_replies=repbool, include_rts=rtsbool).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_retweets_of_tweets(self, num_rts=100):
        retweets = []
        retweets_list = self.twitter_client.get_retweets(id=self.tweet_id, count=num_rts)
        for retweet in retweets_list:
            retweets.append(retweet)
        return retweets

    def get_tweet_by_id(self, ids):
        tweets = []
        for i in ids:
            tweets.append(self.twitter_client.get_status(id=i))
        return tweets


class TwitterClientV2:
    def __init__(self, apis, twitter_user=None, tweet_id=None):
        self.twitter_client = TwitterAuthenticatorV2().authenticate_twitter_app(api=apis)
        self.twitter_user = twitter_user
        self.tweet_id = tweet_id

    def get_mentions(self, end_date, num_mentions=100):
        query = f'@{self.twitter_user} -is:retweet -is:reply'
        tweets = self.twitter_client.search_recent_tweets(query=query,
                                                          # end_time=end_date,
                                                          tweet_fields=["id"],
                                                          max_results=num_mentions)
        return tweets

    def get_replies(self, num_replies=100):
        query = f'in_reply_to_tweet_id:{self.tweet_id}'
        tweets = self.twitter_client.search_recent_tweets(query=query,
                                                          tweet_fields=["id"],
                                                          max_results=num_replies)
        return tweets

    def get_id(self):
        user = self.twitter_client.get_user(username=self.twitter_user)
        return user.data.id

    def get_tweets(self, num_tweets=100):
        tweets = self.twitter_client.get_users_tweets(id=self.get_id(), max_results=num_tweets)
        return tweets


class TwitterAuthenticatorV1:
    @staticmethod
    def authenticate_twitter_app(api):
        index = 0
        auth = OAuthHandler(api[0][index], api[1][index])
        auth.set_access_token(api[2][index], api[3][index])
        return auth


class TwitterAuthenticatorV2:
    @staticmethod
    def authenticate_twitter_app(api):
        index = 2
        client = tweepy.Client(bearer_token=api[4][index],
                               consumer_key=api[0][index],
                               consumer_secret=api[1][index],
                               access_token=api[2][index],
                               access_token_secret=api[3][index],
                               wait_on_rate_limit=True)
        return client
