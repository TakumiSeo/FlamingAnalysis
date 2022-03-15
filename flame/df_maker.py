import mysql.connector
import pandas as pd
import numpy as np
import datetime

def create_df(mysql_info, day):
    '''
    MySQLに接続し，データフレームの作成
    '''
    day = 'day' + str(day)
    mydb = mysql.connector.connect(host=mysql_info['DB_HOST'], port=3306,
                                    db=mysql_info['DB_NAME'], user=mysql_info['DB_USER'],
                                    passwd=mysql_info['DB_PASSWORD'], charset=mysql_info['CHARSET'])

    mycursor = mydb.cursor(buffered=True)
    # SQLクエリ実行（データ取得）
    # created_at が
    sql_out = 'SELECT * FROM %s;' % (day)
    mycursor.execute(sql_out)
    # 表示
    rows = mycursor.fetchall()
    mydb.close()
    mycursor.close()
    # Create DataFrame
    df = pd.DataFrame(list(rows))
    df = df.rename(columns={0: 'tweet_id', 1: 'day_id', 2:'text', 3:'created_at', 4:'user_friends',
          5:'user_followers', 6:'retweet_count', 7:'favorite_count', 8:'AWS', 9:'happy',
          10:'sad', 11:'disgust', 12:'angry', 13:'surprise', 14:'day', 15:'risk'})
    return df

def making_df(data, info_list, column):
    '''
    リストをデータフレームに起こす
    '''
    df = data.copy()
    for name in list(column):
        for i, cont in enumerate(info_list):
            df[name][i] = cont[name]
    return df

def neg_df_requests_API(day, tw, df, df_in=True):
    '''
    テーブルdayXのデータをcreate_dfでDataFrameに変換
    そして，そのテーブルのTweetIDに対応するTweetをAPIをgetTweetDataを通して取得
    そのデータをまたDataFrameに変換し，炎上判定(risk値の更新...Fav,RT数が変わっている場合があるため)する
    ＊sentiment.pyにつながる
    '''
    neg_df = df
    print('df_in, No')
    neg_id_list = [tw.get_tweet_id_data(ID) for ID in neg_df.tweet_id.tolist()]
    df_day = pd.DataFrame(np.arange(7 * len(neg_id_list)).reshape(len(neg_id_list), 7))
    df_day.columns = ['tweet_id', 'created_at', 'user_friends',
                      'user_followers', 'retweet_count', 'favorite_count', 'text']
    neg_df_take_over = neg_df[['tweet_id', 'day_id',  'AWS', 'happy', 'sad', 'disgust', 'angry', 'surprise','risk']]
    df_day = making_df(df_day, neg_id_list, df_day.columns)
    df_day = df_day.dropna()
    df_day = pd.merge(df_day.copy(), neg_df_take_over, on='tweet_id')
    day += 1
    print('day:{}'.format(day))
    df_day['day'] = day
    return df_day
