import twitterClientManager
import twitterClientManagerUpdate
import tweet_utils
import pandas as pd
import numpy as np
import re
import json
import warnings
from keras.models import load_model
from keras_preprocessing.sequence import pad_sequences
import importlib
importlib.reload(twitterClientManagerUpdate)

warnings.filterwarnings("ignore")
path = r"C:\Users\gianluca.nogara\Desktop\Repo\precollection_trolls"

lst_keys = [
    "id",
    "id_str",
    "user.screen_name",
    "text",
    "created_at",
    "user.statuses_count",
    "user.followers_count",
    "user.friends_count",
    "user.favourites_count",
    "retweeted_status.user.screen_name",
    "retweeted_status.user.id",
    "retweet_count",
    "retweet_status.favorite_count",
    "retweet_reply_count",
    "retweeted_status.user.statuses_count",
    "retweeted_status.user.followers_count",
    "retweeted_status.user.friends_count",
    "retweeted_status.user.favourites_count",
    "mention",
    "in_reply_to_screen_name",
    "in_reply_to_status_id_str"
]
lst_mapped = [
    "id",
    "id_str",
    "screen_name",
    "text",
    "created_at",
    "user_statuses_count",
    "user_followers_count",
    "user_friends_count",
    "user_favourites_count",
    "user_retweeted",
    "retweet_id_str",
    "retweet_count",
    "retweet_favorite_count",
    "retweet_reply_count",
    "user_retweeted_statuses_count",
    "user_retweeted_followers",
    "user_retweeted_friends",
    "user_retweeted_favourites_count",
    "mention",
    "in_reply_to_screen_name",
    "in_reply_to_status_id_str"
]

loaded_model = load_model(path + r"\classifier_trajectory_200.h5")


def refactor_columns(df):
    for i in lst_keys:
        if i in list(df.columns):
            continue
        else:
            df[i] = [np.NaN for i in range(len(df))]
    df = df[lst_keys]
    df.columns = lst_mapped
    return df


def parse_output(dataframe):
    if not "'result_count': 0" in str(dataframe[-1:][0]):
        json_data = [r._json for r in dataframe]
        df = pd.json_normalize(json_data)
        df.rename(columns={"full_text": "text"}, inplace=True)
    else:
        df = pd.DataFrame()
    return df


def parse_output_v2(dataframe):
    if len(dataframe) > 0:
        json_data = [r._json for r in dataframe]
        df = pd.json_normalize(json_data)
    else:
        df = pd.DataFrame()
    return df


def get_keys():
    jsonFile = open(path + r'\auth.json', 'r')
    config = json.load(jsonFile)
    jsonFile.close()

    consumer_keys = []
    consumer_secrets = []
    access_tokens = []
    access_token_secrets = []
    bearer_tokens = []

    for i in config["DEFAULT"]["twitter_credentials"]:
        consumer_keys.append(i["consumer_key"])
        consumer_secrets.append(i["consumer_secret"])
        access_tokens.append(i["access_token"])
        access_token_secrets.append(i["access_token_secret"])
        bearer_tokens.append(i["bearer_token"])

    apis = [consumer_keys, consumer_secrets, access_tokens, access_token_secrets, bearer_tokens]
    return apis


def collect_data_update(username):
    # print("Starting collection...")
    api = get_keys()
    twitter_client_v1 = twitterClientManagerUpdate.TwitterClientV1(apis=api, twitter_user=username)
    idd, original, reply, retweet, mentions = twitter_client_v1.get_active_interactions()
    # print(f"Original: ", len(original))
    # print(f"Reply: ", len(reply))
    # print(f"Retweet: ", len(retweet))
    # print(f"Mentions: ", len(mentions))
    rts = twitter_client_v1.get_passive_rts(idd)
    # print(f"RTs: ", len(rts))
    rps = twitter_client_v1.get_passive_rps(idd)
    # print(f"RPs: ", len(rps))
    mnts = twitter_client_v1.get_passive_mentions(idd)
    # print(f"MNTs: ", len(mnts))
    # print("Data collected!")
    # print("-----------------")
    return original, mentions, reply, retweet, rts, rps, mnts


def collect_data(username):
    print("Starting collection...")
    api = get_keys()
    tweets_number = 10
    retweets_number = 5
    mention_number = 10
    reply_number = 10
    user = username
    date = ""

    tweets = []
    retweets = []
    mentions = []
    replies = []

    tweets_ids = []
    replies_ids = []
    mentions_ids = []
    twitter_client_v1 = twitterClientManager.TwitterClientV1(api)
    twitter_client_v2 = twitterClientManager.TwitterClientV2(api, user)

    print("Collecting tweets for ", user)
    tweets.extend(twitter_client_v2.get_tweets(tweets_number))
    if not "'result_count': 0" in str(tweets[-1:][0]):
        for i in tweets[0]:
            tweets_ids.append(str(i.keys).split("=")[1].split(" ")[0])
        tweets = twitter_client_v1.get_tweet_by_id(tweets_ids)
    else:
        print("No tweets...")

    print("Collecting retweets received for ", user)
    for i in tweets:
        twitter_client_v1 = twitterClientManager.TwitterClientV1(api, None, i.id)
        retweets.extend(twitter_client_v1.get_retweets_of_tweets(retweets_number))
        date = i.created_at
    if len(retweets) == 0:
        print("No retweets...")

    print("Collecting mentions received for ", user)
    twitter_client_v2 = twitterClientManager.TwitterClientV2(api, user)
    mentions.extend(twitter_client_v2.get_mentions(date, mention_number))
    if not "'result_count': 0" in str(mentions[-1:][0]):
        for i in mentions[0]:
            mentions_ids.append(str(i.keys).split("=")[1].split(" ")[0])
        mentions = twitter_client_v1.get_tweet_by_id(mentions_ids)
    else:
        print("No mentions...")

    print("Collecting replies received for", user)
    for i in tweets:
        twitter_client_v2 = twitterClientManager.TwitterClientV2(api, None, i.id)
        replies.extend(twitter_client_v2.get_replies(reply_number))
    if not "'result_count': 0" in str(mentions[-1:][0]):
        for i in replies[0]:
            replies_ids.append(str(i.keys).split("=")[1].split(" ")[0])
        replies = twitter_client_v1.get_tweet_by_id(replies_ids)
    else:
        print("No replies")
    print("Data collected!")
    print("-----------------")
    return tweets, retweets, mentions, replies


def dataset_split(df_activities, df_mentions, df_rps, df_rts, username):
    original = df_activities[(df_activities["retweet_id_str"].isna()) &
                             (df_activities["in_reply_to_status_id_str"].isna())]
    for i, row in original.iterrows():
        result = re.findall("@([a-zA-Z0-9]{1,15})", row.text)
        if len(result) > 0:
            original.at[i, "mention"] = result[0]

    for i, row in df_mentions.iterrows():
        df_mentions.at[i, "mention"] = username
    df_tw_a = df_activities[
        (df_activities["retweet_id_str"].isna()) &
        (df_activities["in_reply_to_status_id_str"].isna()) &
        (df_activities["mention"].isna())]

    df_m_a = df_activities[df_activities["mention"].notna()]
    df_rp_a = df_activities[df_activities["in_reply_to_status_id_str"].notna()]
    df_RT_a = df_activities[
        (df_activities["in_reply_to_status_id_str"].isna()) &
        (df_activities["retweet_id_str"].notna())]
    df_RT_m = df_rts
    df_rp_m = df_rps
    df_m_m = df_mentions
    return df_tw_a, df_m_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m


def generate_trajectory(df_tw_a, df_m_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m):
    df_author = df_RT_a.append(df_rp_a, ignore_index=True)
    df_author = df_author.append(df_tw_a, ignore_index=True)
    df_author = df_author.append(df_m_a, ignore_index=True)
    df_author.reset_index(inplace=True)
    users_subset = pd.unique(df_author.screen_name)

    # threshold parameter
    t_a = 5  # threshold to filter out trolls (resp. users) that have not performed at least t_a sharing activities
    # (original tweets +retweets + replies + mentions)
    t_p = 5  # threshold to filter out trolls (resp. users) that have not received at least t_p feedback from other
    # accounts (retweets + replies + mentions)
    traj = []
    user = []
    for user_name in users_subset:
        # print(f"creating traj for {user_name}")

        tw_a_u, n_tw_a = tweet_utils.create_user_dataframe(df_tw_a, user_name, 'screen_name', 1,
                                                           "a")  # action: original tweet
        RT_a_u, n_rt_a = tweet_utils.create_user_dataframe(df_RT_a, user_name, 'screen_name', 2, "a")  # action: retweet
        rp_a_u, n_rp_a = tweet_utils.create_user_dataframe(df_rp_a, user_name, 'screen_name', 3, "a")  # action: reply
        m_a_u, n_m_a = tweet_utils.create_user_dataframe(df_m_a, user_name, 'screen_name', 4, "a")  # action: mention
        n_a = n_tw_a + n_rt_a + n_rp_a + n_m_a  # number of actions

        # states
        RT_p_u, n_rt_p = tweet_utils.create_user_dataframe(df_RT_m, user_name, 'user_retweeted', 5,
                                                           "p")  # state: got a retweet
        rp_p_u, n_rp_p = tweet_utils.create_user_dataframe(df_rp_m, user_name, 'in_reply_to_screen_name', 6,
                                                           "p")  # state: got a reply
        m_p_u, n_m_p = tweet_utils.create_user_dataframe(df_m_m, user_name, 'mention', 7, "p")  # state: got a mention
        n_p = n_rt_p + n_rp_p + n_m_p  # number of states

        if n_a > t_a and n_p > t_p:  # filtering out some users

            # creating actions dataframe
            df_active_u = tw_a_u.append(RT_a_u, ignore_index=True)
            df_active_u = df_active_u.append(rp_a_u, ignore_index=True)
            df_active_u = df_active_u.append(m_a_u, ignore_index=True)
            df_active_u = df_active_u.sort_values(by='time')
            df_active_u.reset_index(inplace=True)
            del df_active_u['index']
            df_active_u = tweet_utils.adjust_tweet_df(df_active_u)

            # creating states dataframe
            RT_p_u = tweet_utils.adjust_retweet_df(RT_p_u)
            rp_p_u = tweet_utils.adjust_reply_df(rp_p_u)
            m_p_u = tweet_utils.adjust_mention_df(m_p_u)
            df_passive_u = RT_p_u.append(rp_p_u, ignore_index=True)
            df_passive_u = df_passive_u.append(m_p_u, ignore_index=True)
            df_passive_u = df_passive_u.sort_values(by='time')
            df_passive_u.reset_index(inplace=True)
            df_passive_u = df_passive_u[df_passive_u["tweet_id"].notna()]
            del df_passive_u['index']

            # create total dataframe
            df_total = tweet_utils.merge_df(df_active_u, df_passive_u)

            # IRL
            trajectories, state_sequence, n_states, n_actions, feature_matrix, t_dict, \
            c_dict, traj_lst = tweet_utils.tweet_traj_next_reduced(df_total)  # compute trajectories and other
            traj.extend(traj_lst)
            user.append(user_name)
        else:
            print("user %s has %s actions and %s states" % (user_name, n_a, n_p))
    # print("Traj calculated!")
    # print("-----------------")
    return pd.DataFrame(list(zip(user, traj)), columns=["screen_name", "state_sequence"])


def reconstruct_traj(row):
    f_i = row
    f_i = f_i[1:]
    f_i = f_i[:-1]
    s = str(f_i)
    s = s.replace(" ", "")
    x = s.split("[")
    t = []
    for i in range(0, len(x)):
        r = x[i].replace("]", "").split(",")
        if len(r) >= 2:
            t.append([r[0], r[1]])
    return t


def remove_action(traj):
    seq = []
    for i in range(len(traj)):
        seq.append(int(traj[i][0]))
    return seq


def predict(sequence):
    max_length = 200
    sequence = pad_sequences(sequence, maxlen=max_length, padding='post')
    sequence = sequence.reshape(sequence.shape[0], sequence.shape[1], 1)
    return loaded_model.predict(sequence)


def calculate_troll_score(username, collection=True, df_tw_a=None, df_m_a=None, df_rp_a=None, df_RT_a=None,
                          df_RT_m=None, df_rp_m=None, df_m_m=None):
    if collection:
        df_tw_a, df_m_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m = collect_data_update(username)
        # df_activities = parse_output(tweets)
        # df_rts = parse_output_v2(retweets)
        # df_mentions = parse_output(mentions)
        # df_rps = parse_output(replies)

        # df_activities = refactor_columns(df_activities)
        # df_mentions = refactor_columns(df_mentions)
        # df_rps = refactor_columns(df_rps)
        # df_rts = refactor_columns(df_rts)
        # df_tw_a, df_m_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m = dataset_split(df_activities, df_mentions,
        # df_rps, df_rts, username)
        df_tw_a = refactor_columns(df_tw_a)
        df_m_a = refactor_columns(df_m_a)
        df_rp_a = refactor_columns(df_rp_a)
        df_RT_a = refactor_columns(df_RT_a)
        df_RT_m = refactor_columns(df_RT_m)
        df_rp_m = refactor_columns(df_rp_m)
        df_m_m = refactor_columns(df_m_m)

    df_traj = generate_trajectory(df_tw_a, df_m_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m)
    if len(df_traj) == 0:
        print(f"{username} doesn't have enough interaction, cannot calculate Troll score")
        return None
    else:
        df_traj['state_sequence'] = df_traj.apply(lambda row: reconstruct_traj(row['state_sequence']), axis=1)
        df_traj['sequence_numbers'] = df_traj.apply(lambda row: remove_action(row['state_sequence']), axis=1)
        # print(df_traj["sequence_numbers"]
        names = list(df_traj["screen_name"])
        lst = predict(df_traj["sequence_numbers"])
        for i in range(len(names)):
            print(names[i])
            print(lst[i][0])
            print("_________")
        # return predict(df_traj["sequence_numbers"])[0][0]
