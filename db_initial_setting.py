import mysql.connector
import datetime
from flame.utils import import_conofiguration as icnf

config = icnf(config_file='flame/setting.ini')
mysql_info = {'DB_HOST': config['DB_HOST'], 'DB_NAME': config['DB_NAME'],
            'DB_USER': config['DB_USER'], 'DB_PASSWORD': config['DB_PASSWORD']
             ,'CHARSET': config['CHARSET']}


class DBInitialInsert:
    def __init__(self, mysql_info):
        self.mydb =  mysql.connector.connect(host=mysql_info['DB_HOST'], port=3306,
                                    db=mysql_info['DB_NAME'], user=mysql_info['DB_USER'],
                                    passwd=mysql_info['DB_PASSWORD'], charset=mysql_info['CHARSET'])
        self.cursor = self.mydb.cursor()

    def create_table(self, day):
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
        #self.mydb.commit()
        #self.execute_sql(sql, is_commit=True)
        return True

    def insert_into(self, search_dict, day):
        '''
        作成したテーブル内にデータを挿入
        '''
        day = 'day' + str(day)
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
        #self.mydb.commit()
        # self.execute_sql(sql, is_commit=True)
        return True

    def initial_value(self, day_number):
        dict_init = {'tweet_id': 1493139843316023301,
                    'day_id': datetime.date.today()
                                -datetime.timedelta(days=day_number),
                    'text': 'gakken good',
                    'created_at': datetime.datetime.now(),
                    'user_friends': 0,
                    'user_followers': 0,
                    'retweet_count': 10,
                    'favorite_count': 10,
                    'AWS': 'NEGATIVE',
                    'happy': 0,
                    'sad': 0,
                    'disgust': 0,
                    'angry': 0,
                    'surprise': 0,
                    'day': 0,
                    'risk': 2}
        return dict_init

    def close_all(self):
        self.mydb.close()
        self.cursor.close()

    def create_hashtag_serch_table(self):
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
        self.cursor.execute(sql)
        self.mydb.commit()
        return True

    def run(self):
        # create TABLE
        for d in range(0, 7):
            self.create_table(d)
        print('Now done creating table\n')

        for day_number in range(0, 7):
            if (0 <= day_number) and  (day_number <= 6):
                print('yes')
                self.insert_into(self.initial_value(day_number=day_number), 0)
            if 1 <= day_number <= 6:
                self.insert_into(self.initial_value(day_number=day_number), 1)
            if 2 <= day_number <= 6:
                self.insert_into(self.initial_value(day_number=day_number), 2)
            if 3 <= day_number <= 6:
                self.insert_into(self.initial_value(day_number=day_number), 3)
            if 4 <= day_number <= 6:
                self.insert_into(self.initial_value(day_number=day_number), 4)
            if 5 <= day_number <= 6:
                self.insert_into(self.initial_value(day_number=day_number), 5)
            if day_number == 6:
                self.insert_into(self.initial_value(day_number=day_number), 6)
            else:
                pass

db_ini =  DBInitialInsert(mysql_info)
db_ini.run()
db_ini.create_hashtag_serch_table()
db_ini.close_all()
