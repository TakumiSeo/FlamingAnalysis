import re
from datetime import datetime, timedelta, date
import json
import emoji
import numpy as np

class TweetGetter:

    def __init__(self, twitter):
        self.twitter = twitter

    def tweet_cleaner(self, tweet):
        """
        textをクリーニング
        """
        tweet = re.sub("@[A-Za-z0-9_]+", "", tweet)  # Remove @ sign
        tweet = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", tweet)  # Remove http links
        tweet = re.sub("Retweet", "", tweet)
        tweet = re.sub("retweet", "", tweet)
        tweet = " ".join(tweet.split())
        tweet = ''.join(c for c in tweet if c not in emoji.UNICODE_EMOJI)  # Remove Emojis
        # tweet = tweet.replace("#", "").replace("_", " ") #Remove hashtag sign but keep the text
        tweet = tweet.replace("#", "").replace("_", " ")
        tweet = tweet.replace("RT", "")
        tweet = tweet.replace(":", "")
        tweet = tweet.replace('"', '')
        tweet = tweet.replace("'", "")
        tweet = tweet.replace("[.]+", "")
        tweet = tweet.replace("…", "")
        tweet = tweet.lstrip()
        return tweet

    def get_tweet_id_data(self, ID):
        '''
        tweetデータの取得
        '''
        url = 'https://api.twitter.com/1.1/statuses/show.json?id={}'.format(ID)
        # 取得したデータの分解
        try:  # 成功した場合
            req = self.twitter.get(url)
            data = req.json()
            print(data['created_at'])
            x = datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y") + timedelta(hours=9)
            # datetimeの形を使いやすい形に
            data['created_at'] = '{0:%Y-%m-%d-%H-%M-%S}'.format(x)
            data['user']['created_at'] = '{0:%Y-%m-%d-%H-%M-%S}'.format(
                datetime.strptime(data['user']['created_at'], "%a %b %d %H:%M:%S %z %Y") + timedelta(hours=9))
            return {'tweet_id': ID, 'created_at': data['created_at'], 'user_id': int(data['user']['id']),
                    "user_name": data['user']['screen_name'], "user_friends": data['user']['friends_count'],
                    "user_followers": data['user']['followers_count'], 'retweet_count': data['retweet_count'],
                    'favorite_count': data['favorite_count'], "text": self.tweet_cleaner(data['text'])}

        except:  # 失敗した場合
            print('The ID {} being taken now has been deleated by the user'.format(ID))
            return {'tweet_id': ID, 'created_at': np.nan, 'user_id': np.nan,
                    "user_name": np.nan, "user_friends": np.nan,
                    "user_followers": np.nan, 'retweet_count': np.nan,
                    'favorite_count': np.nan, "text": np.nan}

    def get_zeroday_tweet_data(self, search_word, max_id, since_id, start_date):
        '''
        tweetデータの取得
        '''
        url = 'https://api.twitter.com/1.1/search/tweets.json'
        # ダブりをなくすために　+1分
        start_date = start_date + timedelta(minutes=1)
        # dateの表記をTwitterAPIに合わせる
        start_date = '20' + start_date.strftime('%y-%m-%d_%H:%M:%S') + '_JST'
        params = {'q': search_word,
                  'count': '100',
                  'since': start_date
                  }
        # max_idの指定があれば設定する
        if max_id != -1:
            params['max_id'] = max_id
        # since_idの指定があれば設定する
        if since_id != -1:
            params['since_id'] = since_id
        # Tweetデータの取得
        req = self.twitter.get(url, params=params)
        # 取得したデータの分解
        if req.status_code == 200:  # 成功した場合
            timeline = json.loads(req.text)
            data = req.json()['statuses']
            metadata = timeline['search_metadata']
            # datetimeの形を使いやすい形に
            for i in range(len(data)):
                data[i]['created_at'] = '{0:%Y-%m-%d-%H-%M-%S}'.format(
                    datetime.strptime(data[i]['created_at'], "%a %b %d %H:%M:%S %z %Y") + timedelta(hours=9))
                data[i]['user']['created_at'] = '{0:%Y-%m-%d-%H-%M-%S}'.format(
                    datetime.strptime(data[i]['user']['created_at'], "%a %b %d %H:%M:%S %z %Y") + timedelta(hours=9))
            limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
            reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
            return {"result": True, "metadata": metadata, "statuses": data, "limit": limit,
                    "reset_time": datetime.fromtimestamp(float(reset)), "reset_time_unix": reset}
        else:  # 失敗した場合
            print("Error: %d" % req.status_code)
            return {"result": False, "status_code": req.status_code}