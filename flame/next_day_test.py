import mysql.connector

class DBController:
    def __init__(self, mysql_info):
        self.my_db_name = mysql_info['DB_NAME']
        self.my_db_char = mysql_info['CHARSET']
        self.mydb = mysql.connector.connect(host=mysql_info['DB_HOST'],port=3306,
                                            db=mysql_info['DB_NAME'],user=mysql_info['DB_USER'],
                                            passwd=mysql_info['DB_PASSWORD'],charset=mysql_info['CHARSET'])
        self.cursor = self.mydb.cursor()
  
    def db_close(self):
        self.mydb.close()

    def create_db(self):
        '''
        MySQLデータベースの作成
        '''
        sql = u"""
        CREATE DATABASE IF NOT EXISTS                                                                                                                                 
            %s                                                                                                                                                        
        CHARACTER SET                                                                                                                                                 
            %s                                                                                                                                                  
        ;                                                                                                                                                             
        """ % (self.my_db_name, self.my_db_char)
        self.cursor.execute(sql)
        self.mydb.commit()
        return True


    def create_hashtag_serch_table(self, day):
        '''
        database内にtableを作成
        '''
        day = 'day' + str(day)
        sql = """                                                                                                                                                     
        CREATE TABLE IF NOT EXISTS                                                                                                                                    
            %s(                                                                                                                                                                                        
                tweet_id BIGINT,
                day_id DATETIME,
                text VARCHAR(255),
                created_at DATETIME,
                user_friends MEDIUMINT,
                user_followers MEDIUMINT,
                retweet_count MEDIUMINT,                                                                                                                                       
                favorite_count MEDIUMINT,
                AWS CHAR(10),
                happy TINYINT,
                sad TINYINT,
                disgust TINYINT,
                angry TINYINT,
                surprise TINYINT,
                day TINYINT,
                risk TINYINT
            )                                                                                                                                                         
        ;                                                                                                                                                             
        """ % (day)
        self.cursor.execute(sql)
        self.mydb.commit()
        return True


    def drop_table(self):
        sql = '''DROP TABLE IF EXISTS negative'''
        self.execute_sql(sql, is_commit=True)
        return True


    def insert_into_hashtag_search(self, search_dict, day, day_bool=True):
        '''
        作成したテーブル内にデータを挿入
        '''
        if day_bool:
            day = 'day' + str(day)
        else:
            day = 'all_data'
        sql = """                                                                                                                                                     
        INSERT INTO                                                                                                                                                   
            %s                                                                                                                                           
        VALUES(                                                                                                                                                       
            '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'                                                                         
            )                                                                                                                                                         
        ;                                                                                                                         
        """ % (
            day,
            search_dict["tweet_id"],
            search_dict['day_id'],
            search_dict["text"],
            search_dict["created_at"],
            search_dict["user_friends"],
            search_dict["user_followers"],
            search_dict["retweet_count"],
            search_dict["favorite_count"],
            search_dict["AWS"],
            search_dict["happy"],
            search_dict["sad"],
            search_dict["disgust"],
            search_dict["angry"],
            search_dict["surprise"],
            search_dict["day"],
            search_dict["risk"]
        )
        self.cursor.execute(sql)
        self.mydb.commit()
        return True


    def day_zero_db(self):
        '''
        day0テーブル専用の関数
        return テーブル内の一番新しい日付
        '''
        # データベースを初期作成の際に，パラメータのsinceを調整する為
        # SQLクエリ実行（データ取得）
        sql_out = 'SELECT MAX(created_at) FROM day0';
        self.cursor.execute(sql_out)
        # データの取得
        rows = self.cursor.fetchall()
        # MySQLの切断
        return rows


    def next_day(self, df, day, put_zero=False):
        '''
        sentiment.py のflaming_classifierからdf', day
        '''
        # テーブル作成
        if put_zero:
            day = 0
        else:
            day += 1
        for index, row in df.iterrows():
            hashtag_search_dict = {
                "tweet_id": u"{}".format(row.tweet_id),
                'day_id': u"{}".format(row.day_id),
                "text": u"{}".format(row.text),
                "created_at": u"{}".format(row.created_at),
                "user_friends": u"{}".format(row.user_friends),
                "user_followers": u"{}".format(row.user_followers),
                "retweet_count": u"{}".format(row.retweet_count),
                "favorite_count": u"{}".format(row.favorite_count),
                "AWS": u"{}".format(row.AWS),
                "happy": u"{}".format(row.happy),
                "sad": u"{}".format(row.sad),
                "disgust": u"{}".format(row.disgust),
                "angry": u"{}".format(row.angry),
                "surprise": u"{}".format(row.surprise),
                "day": u"{}".format(day),
                "risk": u"{}".format(row.risk)
            }
            self.insert_into_hashtag_search(hashtag_search_dict, day=day)

    def store_all_data(self, df):
        sql = """                                                                                                                                                     
        CREATE TABLE IF NOT EXISTS                                                                                                                                    
            %s(                                                                                                                                                                                        
                tweet_id BIGINT,
                day_id DATETIME,
                text VARCHAR(255),
                created_at DATETIME,
                user_friends MEDIUMINT,
                user_followers MEDIUMINT,
                retweet_count MEDIUMINT,                                                                                                                                       
                favorite_count MEDIUMINT,
                AWS CHAR(10),
                happy TINYINT,
                sad TINYINT,
                disgust TINYINT,
                angry TINYINT,
                surprise TINYINT,
                day TINYINT,
                risk TINYINT
            )                                                                                                                                                         
        ;                                                                                                                                                             
        """ % ('all_data')
        self.cursor.execute(sql)
        self.mydb.commit()

        for index, row in df.iterrows():
            hashtag_search_dict = {
                "tweet_id": u"{}".format(row.tweet_id),
                'day_id': u"{}".format(row.day_id),
                "text": u"{}".format(row.text),
                "created_at": u"{}".format(row.created_at),
                "user_friends": u"{}".format(row.user_friends),
                "user_followers": u"{}".format(row.user_followers),
                "retweet_count": u"{}".format(row.retweet_count),
                "favorite_count": u"{}".format(row.favorite_count),
                "AWS": u"{}".format(row.AWS),
                "happy": u"{}".format(row.happy),
                "sad": u"{}".format(row.sad),
                "disgust": u"{}".format(row.disgust),
                "angry": u"{}".format(row.angry),
                "surprise": u"{}".format(row.surprise),
                "day": u"{}".format('nan'),
                "risk": u"{}".format(row.risk)
            }
            self.insert_into_hashtag_search(hashtag_search_dict, day='all_data', day_bool=False)

    def all_close(self):
        self.mydb.close()
        self.cursor.close()