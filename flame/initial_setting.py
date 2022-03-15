#!/usr/bin/env python
import re
from datetime import datetime, timedelta, date
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import time, sys, traceback
import mysql.connector
from tweet_getter import TweetGetter
from requests_oauthlib import OAuth1Session



'''
crontab -eの場合は以下のimport
'''
DB_USER = ***
DB_PASSWORD = ***
DB_HOST = ***
DB_NAME = ***
CHARSET = ***

TARGET_WORD = ***
CONSUMER_KEY = ***
CONSUMER_SECRET = ***
ACCESS_TOKEN = ***
ACCESS_TOKEN_SECRET = ***

twitter = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
tw = TweetGetter(twitter)
# TwitterAPIのアクセスキートークン

exception_list = []
# 関係の無い文字列のリスト
noise_list = ['*']



def now_unix_time():
    return time.mktime(datetime.now().timetuple())


def execute_sql(sql, db_info, is_commit=False):
    '''
    SQL文の実行
    '''
    connector = mysql.connector.connect(
        host=db_info["host"],
        port=*,
        user=db_info["user"],
        password=db_info["password"],
        db=db_info["db_name"],
        charset="utf8"
    )
    cursor = connector.cursor()
    cursor.execute(sql)
    if is_commit:
        connector.commit()
    cursor.close()
    connector.close()
    return True


def create_hashtag_serch_table(db_info):
    '''
    database内にtableを作成
    '''
    sql = """
    CREATE TABLE IF NOT EXISTS
        initial_day(
            tweet_id BIGINT,
            day_id DATETIME,
            created_at DATETIME,
            user_id BIGINT,
            user_name VARCHAR(50),
            user_friends MEDIUMINT,
            user_followers MEDIUMINT,
            retweet_count MEDIUMINT,
            favorite_count MEDIUMINT,
            text VARCHAR(255)
        )
    ;
    """
    execute_sql(sql, db_info, is_commit=True)
    return True


def insert_into_hashtag_search(db_info, hashtag_search_dict):
    '''
    作成したテーブル内にデータを挿入
    '''
    sql = """
    INSERT INTO
        initial_day
    VALUES(
        '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' , '%s'
        )
    ;
    """ % (
        hashtag_search_dict["tweet_id"],
        hashtag_search_dict['day_id'],
        hashtag_search_dict["created_at"],
        hashtag_search_dict["user_id"],
        hashtag_search_dict["user_name"],
        hashtag_search_dict["user_friends"],
        hashtag_search_dict["user_followers"],
        hashtag_search_dict["retweet_count"],
        hashtag_search_dict["favorite_count"],
        hashtag_search_dict["text"]
    )
    execute_sql(sql, db_info, is_commit=True)
    return True


def tweet_main():
    sid = -1
    mid = -1
    count = 0
    week_ago = date.today() - timedelta(days=1)
    local_db = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "db_name": DB_NAME
    }
    # テーブル作成
    create_hashtag_serch_table(local_db)
    while True:
        try:
            count += 1
            sys.stdout.write('%d, ' % count)
            # ここにデータベースがもし存在していたら，そのマックスの時間を入れて，その時間以上のものからログデータを取らせる様にする
            tweet_data = tw.get_zeroday_tweet_data(u'学研', max_id=mid, since_id=sid, start_date=week_ago)
            if tweet_data['result'] == False:
                print("status_code{}".format(tweet_data['status_code']))
                break

            if int(tweet_data['limit']) == 0:
                print('Adding created_at field')
                diff_sec = int(tweet_data['reset_time_unix']) - now_unix_time()
                print("sleep %d sec." % (diff_sec + 5))
                break
            else:
                # metadata処理
                if len(tweet_data['statuses']) == 0:
                    sys.stdout.write("statuses is none.")
                    break
                elif 'next_results' in tweet_data['metadata']:

                    # 結果をMySQLに格納する
                    tweet_data_st = tweet_data['statuses']
                    for tweet in tweet_data_st:
                        if (tweet['user']['screen_name'] not in exception_list) & (len(re.findall(r'【*】', tweet['text'])) == 0):
                            if len([s for s in noise_list if s in tweet['text']]) == 0:
                                # データのストア
                                tweet['text'] = tw.tweet_cleaner(tweet['text'])
                                hashtag_search_dict = {
                                    "tweet_id": u"{}".format(tweet['id']),
                                    "day_id": u"{}".format(date.today()),
                                    "created_at": u"{}".format(tweet['created_at']),
                                    "user_id": u"{}".format(tweet['user']['id']),
                                    "user_name": u"{}".format(tweet['user']['screen_name']),
                                    "user_friends": u"{}".format(tweet['user']['friends_count']),
                                    "user_followers": u"{}".format(tweet['user']['followers_count']),
                                    'retweet_count': u'{}'.format(tweet['retweet_count']),
                                    'favorite_count': u'{}'.format(tweet['favorite_count']),
                                    "text": u"{}".format(tweet['text'])
                                }
                                insert_into_hashtag_search(local_db, hashtag_search_dict)

                    next_url = tweet_data['metadata']['next_results']
                    pattern = r".*max_id=([0-9]*)\&.*"
                    ite = re.finditer(pattern, next_url)
                    for i in ite:
                        mid = i.group(1)
                        break
                else:
                    sys.stdout.write("next is none. finished.")
                    break
        except SSLError:
            print("SSLError")
            print("waiting 5mins")
            time.sleep(5 * 60)
        except ConnectionError:
            print("ConnectionError")
            print("waiting 5mins")
            time.sleep(5 * 60)
        except ReadTimeout:
            print("ReadTimeout")
            print("waiting 5mins")
            time.sleep(5 * 60)
        except:
            print("Unexpected error:{}".format(sys.exc_info()[0]))
            traceback.format_exc(sys.exc_info()[2])
            raise
        finally:
            info = sys.exc_info()
    return True

tweet_main()
