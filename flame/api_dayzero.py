#!/usr/bin/env python

import pandas as pd
import numpy as np
import re
import datetime as dt
from datetime import datetime
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import time
import sys
import traceback
import mysql.connector
from df_maker import making_df


class AddDayZero:
    def __init__(self, mysql_info, noise_list, exception_list):
        self.mydb = mysql.connector.connect(host=mysql_info['DB_HOST'], port=3306,
                                            db=mysql_info['DB_NAME'], user=mysql_info['DB_USER'],
                                            passwd=mysql_info['DB_PASSWORD'], charset=mysql_info['CHARSET'])
        self.cursor = self.mydb.cursor()
        self.noise_list = noise_list
        self.exception_list = exception_list

    def now_unix_time(self):
        return time.mktime(datetime.now().timetuple())

    def all_close(self):
        self.mydb.close()
        self.cursor.close()

    def add_dayzero(self, tw, initial_day=False):
        sid = -1
        mid = -1
        count = 0
        tweet_list = []
        if initial_day:
            sql_out = 'SELECT * FROM initial_day'
            try:
                # SQLクエリ実行（データ取得）
                self.cursor.execute(sql_out)
                # 表示
                rows = self.cursor.fetchall()

            except ZeroDivisionError:
                print('Error')

        else:
            sql_out = 'SELECT MAX(created_at) FROM day0'
            # データベースを初期作成の際に，パラメータのsinceを調整する為
            try:
                # SQLクエリ実行（データ取得）
                self.cursor.execute(sql_out)
                # 表示
                rows = self.cursor.fetchall()
                # day0のcreated_atから一番直近の日付をとる．
                rows = rows[0][0]

            except ZeroDivisionError:
                print('Error')

            print('取得開始日 {}'.format(rows))
        if not initial_day:
            while True:
                try:
                    count += 1
                    sys.stdout.write('%d, ' % count)
                    # ここにデータベースがもし存在していたら，そのマックスの時間を入れて，その時間以上のものからログデータを取らせる
                    tweet_data = tw.get_zeroday_tweet_data(
                        u'学研', max_id=mid, since_id=sid, start_date=rows)
                    if tweet_data['result'] == False:
                        print("status_code{}".format(
                            tweet_data['status_code']))
                        break

                    if int(tweet_data['limit']) == 0:
                        print('created_at fieldの作成')
                        diff_sec = int(
                            tweet_data['reset_time_unix']) - self.now_unix_time()
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
                                if (tweet['user']['screen_name'] not in self.exception_list) & (
                                        len(re.findall(r'【*】', tweet['text'])) == 0):
                                    if len([s for s in self.noise_list if s in tweet['text']]) == 0:
                                        # データのストア
                                        tweet['text'] = tw.tweet_cleaner(
                                            tweet['text'])
                                        hashtag_search_dict = {
                                            "tweet_id": u"{}".format(tweet['id']),
                                            "day_id": u"{}".format(dt.date.today()),
                                            "created_at": u"{}".format(tweet['created_at']),
                                            "user_id": u"{}".format(tweet['user']['id']),
                                            "user_name": u"{}".format(tweet['user']['screen_name']),
                                            "user_friends": u"{}".format(tweet['user']['friends_count']),
                                            "user_followers": u"{}".format(tweet['user']['followers_count']),
                                            'retweet_count': u'{}'.format(tweet['retweet_count']),
                                            'favorite_count': u'{}'.format(tweet['favorite_count']),
                                            "text": u"{}".format(tweet['text'])
                                        }

                                        tweet_list.append(hashtag_search_dict)
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
            df = pd.DataFrame(np.arange(10 * len(tweet_list)).reshape(len(tweet_list), 10),
                              columns={'tweet_id', 'day_id', 'created_at', 'user_id', 'user_name', 'user_friends', 'user_followers',
                                       'retweet_count', 'favorite_count', 'text'})
            df = making_df(df, tweet_list, df.columns)

            return df

        else:
            # Create DataFrame
            df_init = pd.DataFrame(list(rows))
            df_init = df_init.rename(columns={0: 'tweet_id', 1: 'day_id', 2: 'created_at', 3: 'user_id', 4: 'user_name',
                                              5: 'user_friends', 6: 'user_followers', 7: 'retweet_count',
                                              8: 'favorite_count', 9: 'text'})
            print('df initial:{}'.format(df_init))
            return df_init

    def get_day_id(self, day, day_id=0, get_day=False, get_matched_id=False):
        if get_day:
            sql_out = 'SELECT MIN(day_id) FROM day{}'.format(str(day))
        if get_matched_id:
            sql_out = "SELECT * FROM {} WHERE day_id = '{}'".format(
                'day' + str(day), day_id)

        try:
            # SQLクエリ実行（データ取得）
            self.cursor.execute(sql_out)
            # 表示
            rows = self.cursor.fetchall()
            if get_day:
                rows = rows[0][0]
            if get_matched_id:
                df = pd.DataFrame(list(rows))
                df = df.rename(columns={0: 'tweet_id', 1: 'day_id', 2: 'text', 3: 'created_at',
                                        4: 'user_friends', 5: 'user_followers', 6: 'retweet_count',
                                        7: 'favorite_count', 8: 'AWS', 9: 'happy', 10: 'sad',
                                        11: 'disgust', 12: 'angry', 13: 'surprise', 14: 'day', 15: 'risk'})
                rows = df
        except ZeroDivisionError:
            print('Error')

        return rows
