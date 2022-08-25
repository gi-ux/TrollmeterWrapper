import pandas as pd
import re
import numpy as np
import warnings
import tqdm
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
from tqdm import tqdm

warnings.filterwarnings("ignore")
workers = 8
print("Reading files")
df = pd.read_csv(r"C:\Users\gianluca.nogara\Desktop\Repo\Vaccines_Discussion_Italy\Italian\files\tweets\tweets.csv",
                 lineterminator="\n", low_memory=False, encoding="utf-8")
lst = list(pd.Series(df["user_screen_name"])[:20])


def process_users(df_: pd.DataFrame):
    futures = []
    results = pd.DataFrame()
    executor = ProcessPoolExecutor(max_workers=workers)
    sublist = np.array_split(df_, workers)
    global df
    del df
    count = 0
    for sc in sublist:
        futures.append(executor.submit(process_user, sc, count))
        count += 1
    futures_wait(futures)
    for fut in futures:
        results = pd.concat([results, (fut.result())], axis=0)
    return results


def process_user(df: pd.DataFrame, count: int) -> pd.DataFrame:
    print("Starting worker ", count)
    original = df[df["in_reply_to_screen_name"].isna() & df["rt_created_at"].isna() & df["quoted_status_id"].isna()]
    reply = df[df["in_reply_to_user_id"].notna() & df["quoted_status_id"].isna()]
    retweet = df[df["rt_created_at"].notna()]
    quote = df[df["quoted_status_id"].notna() & df["rt_created_at"].isna()]
    original = pd.concat([original, quote], axis=0)
    replies_df = pd.DataFrame()
    mention_df = pd.DataFrame()
    for row in tqdm.tqdm(original.itertuples(index=False)):
        result = re.findall("@([a-zA-Z0-9]{1,15})", row.text)
        if result:
            if row.text[0] == "@":
                replies_df = replies_df.append(pd.DataFrame([row], columns=row._fields), ignore_index=True)
            else:
                mention_df = mention_df.append(pd.DataFrame([row], columns=row._fields), ignore_index=True)
    original = original[~original.id.isin(replies_df.id)]
    original = original[~original.id.isin(mention_df.id)]
    original.reset_index(drop=True, inplace=True)
    reply = pd.concat([reply, replies_df], axis=0)
    reply.reset_index(drop=True, inplace=True)
    mention = mention_df
    mention.reset_index(drop=True, inplace=True)
    df_tw_a = original[original["user_screen_name"].isin(lst)]
    df_m_a = mention[mention["user_screen_name"].isin(lst)]
    df_rp_a = reply[reply["user_screen_name"].isin(lst)]
    df_RT_a = retweet[retweet["user_screen_name"].isin(lst)]
    df_RT_m = retweet[retweet["rt_user_screen_name"].isin(lst)]
    df_rp_m = reply[reply["in_reply_to_screen_name"].isin(lst)]
    df_m_m = pd.DataFrame()
    for row in tqdm.tqdm(mention_df.itertuples(index=False)):
        for i in row.mention:
            if i in lst:
                df_m_m = df_m_m.append(pd.DataFrame([row], columns=row._fields), ignore_index=True)
                break
    df_tw_a.rename(columns={'user_screen_name': 'screen_name',
                            'rt_user_followers_count': 'user_retweeted_followers',
                            'rt_user_friends_count': 'user_retweeted_friends',
                            'rt_user_favourites_count': 'user_retweeted_favourites_count',
                            'rt_user_screen_name': 'user_retweeted',
                            'rt_id': 'retweet_id_str',
                            'retweet_count': 'retweet_count',
                            'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)
    df_m_a.rename(columns={'user_screen_name': 'screen_name',
                           'rt_user_followers_count': 'user_retweeted_followers',
                           'rt_user_friends_count': 'user_retweeted_friends',
                           'rt_user_favourites_count': 'user_retweeted_favourites_count',
                           'rt_user_screen_name': 'user_retweeted',
                           'rt_id': 'retweet_id_str',
                           'retweet_count': 'retweet_count',
                           'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)
    df_rp_a.rename(columns={'user_screen_name': 'screen_name',
                            'rt_user_followers_count': 'user_retweeted_followers',
                            'rt_user_friends_count': 'user_retweeted_friends',
                            'rt_user_favourites_count': 'user_retweeted_favourites_count',
                            'rt_user_screen_name': 'user_retweeted',
                            'rt_id': 'retweet_id_str',
                            'retweet_count': 'retweet_count',
                            'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)
    df_RT_a.rename(columns={'user_screen_name': 'screen_name',
                            'rt_user_followers_count': 'user_retweeted_followers',
                            'rt_user_friends_count': 'user_retweeted_friends',
                            'rt_user_favourites_count': 'user_retweeted_favourites_count',
                            'rt_user_screen_name': 'user_retweeted',
                            'rt_id': 'retweet_id_str',
                            'retweet_count': 'retweet_count',
                            'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)

    df_RT_m.rename(columns={'user_screen_name': 'screen_name',
                            'rt_user_followers_count': 'user_retweeted_followers',
                            'rt_user_friends_count': 'user_retweeted_friends',
                            'rt_user_favourites_count': 'user_retweeted_favourites_count',
                            'rt_user_screen_name': 'user_retweeted',
                            'rt_id': 'retweet_id_str',
                            'retweet_count': 'retweet_count',
                            'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)
    df_rp_m.rename(columns={'user_screen_name': 'screen_name',
                            'rt_user_followers_count': 'user_retweeted_followers',
                            'rt_user_friends_count': 'user_retweeted_friends',
                            'rt_user_favourites_count': 'user_retweeted_favourites_count',
                            'rt_user_screen_name': 'user_retweeted',
                            'rt_id': 'retweet_id_str',
                            'retweet_count': 'retweet_count',
                            'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)
    df_m_m.rename(columns={'user_screen_name': 'screen_name',
                           'rt_user_followers_count': 'user_retweeted_followers',
                           'rt_user_friends_count': 'user_retweeted_friends',
                           'rt_user_favourites_count': 'user_retweeted_favourites_count',
                           'rt_user_screen_name': 'user_retweeted',
                           'rt_id': 'retweet_id_str',
                           'retweet_count': 'retweet_count',
                           'rt_favourite_count': 'retweet_favorite_count'}, inplace=True)
    df_m_a["type_df"] = ["df_m_a" for _ in range(len(df_m_a))]
    df_tw_a["type_df"] = ["df_tw_a" for _ in range(len(df_tw_a))]
    df_rp_a["type_df"] = ["df_rp_a" for _ in range(len(df_rp_a))]
    df_RT_a["type_df"] = ["df_RT_a" for _ in range(len(df_RT_a))]
    df_RT_m["type_df"] = ["df_RT_m" for _ in range(len(df_RT_m))]
    df_rp_m["type_df"] = ["df_rp_m" for _ in range(len(df_rp_m))]
    df_m_m["type_df"] = ["df_m_m" for _ in range(len(df_m_m))]
    df_merged = pd.concat([df_m_a, df_tw_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m], axis=0)
    del original, mention, mention_df, reply, quote, df_m_m, df_m_a, df_rp_a, df_rp_m, df_RT_a, df_tw_a, df_RT_m
    print("Finishing worker ", count)
    # return df_merged


def split_df(df: pd.DataFrame):
    print("Script started, DF Loaded!")
    # FILTER DATASET BASED ON USERS YOU WANT TO PROCESS
    status = df.groupby('user_screen_name')["user_screen_name"].count().reset_index(name="user_statuses_count")
    df = df.merge(status, on="user_screen_name", how="left")
    mentions_names = []
    for i in df["text"]:
        result = re.findall("@([a-zA-Z0-9]{1,15})", i)
        mentions_names.append(result)
    df["mention"] = mentions_names
    df["user_followers_count"] = [np.NaN for _ in range(len(df))]
    df["user_friends_count"] = [np.NaN for _ in range(len(df))]
    df["user_favourites_count"] = [np.NaN for _ in range(len(df))]
    df["retweet_reply_count"] = [np.NaN for _ in range(len(df))]
    df["user_retweeted_statuses_count"] = [np.NaN for _ in range(len(df))]
    new_df = process_users(df)
    print(new_df)


if __name__ == '__main__':
    split_df(df=df)