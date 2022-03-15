import time
import numpy as np
import pandas as pd
import datetime as dt
import time
from requests_oauthlib import OAuth1Session
import warnings
import df_maker
from api_dayzero import AddDayZero
from sentiment import SentimentAnalysis
from next_day_test import DBController
from tweet_getter import TweetGetter
from del_db import DelDB
from utils import import_conofiguration as IC
warnings.simplefilter('ignore')

print('Update {}'.format(dt.datetime.today()))
# 初期値設定
exception_list = []

# 任意のワードの入るツイートを削除するためのリスト
noise_list = [***]

# NOTE!! Fill out your own TwitterAPI information and AWS(if you use, not essential)
config = IC(config_file='setting.ini')
# mysql information
mysql_info = {'DB_HOST':config['DB_HOST'] , 'DB_NAME': config['DB_NAME'],
                'DB_USER': config['DB_USER'], 'DB_PASSWORD': config['DB_PASSWORD'] , 'CHARSET': config['CHARSET']}
aws_info = {'KEY_ID': config['AWS_ACCESS_KEY_ID'], 'SECRET_KEY': config['AWS_SECRET_ACCESS_KEY']}

# TwitterAPIのアクセス権限取得
twitter = OAuth1Session(config['CONSUMER_KEY'], config['CONSUMER_SECRET'],config['ACCESS_TOKEN'], config['ACCESS_TOKEN_SECRET'])
tw = TweetGetter(twitter)
ad = AddDayZero(mysql_info, noise_list, exception_list)
nday = DBController(mysql_info)
del_db = DelDB(mysql_info)
# 初日のデータを貯める時のみTrue
initial_day = False
# 計算時間測定
start = np.round(time.time(), 3)

# リスク評価の重み
ANGRY = 0.5
DISGUST = 0.5
HAPPY = 0.7
SURPRISE = 0.3

print('Do u have stickers??')

def risk_eval(df):
    '''
    リスクの評価を５つの感情の数値と，RT数を用いて0,1,2,3の4段階で行う．3がリスク度強
    '''
    df_ = df.copy()
    df_['eval1'] = 0
    df_['eval2'] = 0
    for i in range(len(df_)):
        try:
            df_['eval1'][i] = np.round(float((df_.disgust[i]*DISGUST + df_.angry[i]*ANGRY) / (df_.happy[i]*HAPPY + df_.surprise[i]*SURPRISE)), 3)
        except:
            df_['eval1'][i] = 0.0

        # retweet count は user friend数と相関が0.5な為，炎上に重要なRTのみ残して評価
        df_['eval2'][i] = (0.1 * float(df_['retweet_count'][i])) * float(df_['eval1'][i])
    # リスクの初期化
    df_['risk'] = 0
    # 1回以上のRTで0.41だから，若干neg
    df_.loc[(df_.eval2 >= 0.1), 'risk'] = 1
    # 感情的にnegかつ，RTが10回以上
    df_.loc[((df_.eval1 >= 1) & (df_.eval2 >= df_.eval1)), 'risk'] = 2
    # 感情的にnegかつ，RTが20回以上
    df_.loc[((df_.eval1 >= 2) & (df_.eval2 >= df_.eval1)), 'risk'] = 3
    df_ = df_.drop(['eval1', 'eval2'], axis=1)
    return df_

def flaming_classifier(day_now, day_zero=False):
    '''
    リスク度合いの更新をする関数
    day0テーブル以外
        return dayとリスクレベルを計算したデータフレーム
    day0テーブルの場合
        return リスクレベルを計算したデータフレーム
    '''
    if not day_zero:
        day_now = risk_eval(day_now)
        return day_now
    else:
        day_now['day'] = 0
        day_now['risk'] = 0
        day_now = risk_eval(day_now)
        return day_now



########
# ここに七日分溜まったデータの削除プログラム
########
if not initial_day:
    # 1週間の場合n=6
    n = 6
    target_day = 5
    # target_dayの時間更新の為
    firstLoop = True
    # # day6の1週間目のデータをそのSQLのテーブルから削除するために，対応するIDを取得
    day_id = ad.get_day_id(day=n, get_day=True)
    print(day_id)
    # 削除の実行
    # (改善ポイント)貯める作りにする
    for i in range(n+1):
        del_db.del_sql('day'+str(i), day_id, is_commit=True)
        time.sleep(1)
    del_db.all_close()
    time.sleep(5)

    while target_day >= 0:
        if firstLoop:
            day_id_ = ad.get_day_id(day=target_day, get_day=True)
            day_id_ += dt.timedelta(days=1)
            print('first day id:{}, target:{}'.format(day_id_, target_day))
            firstLoop = False
        else:
            day_id_ += dt.timedelta(days=1)
            print('day id:{}, target:{}'.format(day_id_, target_day))
        tw_old = ad.get_day_id(day=target_day, day_id=day_id_,  get_matched_id=True)
        tw_new = df_maker.neg_df_requests_API(day=target_day, tw=tw, df_in=True, df=tw_old)
        df_next_day = flaming_classifier(tw_new)
        nday.next_day(df_next_day, day=target_day, put_zero=False)
        target_day -= 1

# initial_day=Falseの場合，day0に存在するcreated_atのマックスの日付を取り出し，
# そこからツイッターデータを取得=>return DataFrame
# initial_dayは基本False
df_add = ad.add_dayzero(tw, initial_day=initial_day)
# データベストの接続解除
# ad.all_close()

print('AWS感情分析開始\n')
senti = SentimentAnalysis(df_add, aws_info)
df_aws_pos, df_aws_neg = senti.aws_sentiment()

# for positive tweets
df_aws_pos['happy'] = 0
df_aws_pos['sad'] = 0
df_aws_pos['disgust'] = 0
df_aws_pos['angry'] = 0
df_aws_pos['surprise'] = 0
df_aws_pos['risk'] = 0
print('AWS分析終了\n')
sj = SentimentAnalysis(df_aws_neg, None)
print('SJ感情分析開始\n')
df_sj_neg = sj.sj_sentiment()
print(df_sj_neg)
print('SJ分析終了\n')
df_insert_neg = flaming_classifier(df_sj_neg, day_zero=True)
print('DataFrameをSQLに挿入\n')
nday.next_day(df_insert_neg, day=0, put_zero=True)
nday.store_all_data(pd.concat([df_aws_pos, df_aws_neg], ignore_index=True))
nday.all_close()
print('実行終了 {}分'.format(np.round(np.round(time.time(), 3) - start)/60, 3))
