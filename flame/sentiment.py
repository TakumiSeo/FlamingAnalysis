import numpy as np
import boto3
from sentimentja import Analyzer


class SentimentAnalysis:
    def __init__(self, df, aws_access):
        self.df = df
        self.aws_access = aws_access

    def aws_sentiment(self):
        '''
        AWS comprehend で分析
        '''
        comprehend = boto3.client(service_name='comprehend',
                                  region_name='ap-northeast-1',
                                  aws_access_key_id=self.aws_access['KEY_ID'],
                                  aws_secret_access_key=self.aws_access['SECRET_KEY'])

        df_aws = self.df[["tweet_id", 'day_id', 'user_friends', 'user_followers', 'retweet_count', 'created_at', 'favorite_count',"text"]]
        # 欠損値を含む行の削除
        df_aws = df_aws.dropna(how='any')
        df_aws["AWS"] = np.nan
        df_aws["AWS_pos_score"] = np.nan
        df_aws["AWS_neg_score"] = np.nan
        df_aws = df_aws.reset_index()
        df_aws_tmp = df_aws.copy()

        result_list = []
        # AWS comprehendの実行
        for text in list(df_aws_tmp.text):
            if len(text) == 0:
                text = 'stay positive there are just no words'
            result_list.append(comprehend.detect_sentiment(Text=text, LanguageCode='ja'))

        # DataFrameにAWSの実行結果の挿入
        for i in range(len(df_aws_tmp)):
            df_aws_tmp.AWS[i] = result_list[i].get('Sentiment')
            df_aws_tmp.AWS_pos_score[i] = round(result_list[i].get('SentimentScore').get('Positive'), 4)
            df_aws_tmp.AWS_neg_score[i] = round(result_list[i].get('SentimentScore').get('Negative'), 4)

        df_aws = df_aws_tmp

        #------------------- テスト用 -------------------#
        # df_aws = self.df[["tweet_id", 'day_id', 'user_friends', 'user_followers', 'retweet_count', 'created_at', 'favorite_count',"text"]]
        # # 欠損値を含む行の削除
        # df_aws = df_aws.dropna(how='any')
        # df_aws["AWS"] = 'NEGATIVE'
        # df_aws["AWS_pos_score"] = 1
        # df_aws["AWS_neg_score"] = 1
        # df_aws = df_aws.reset_index()
        # df_aws = df_aws[-10:]
        
        return df_aws[df_aws.AWS == 'POSITIVE'], df_aws[df_aws.AWS == 'NEGATIVE']

    def sj_sentiment(self):
        '''
        Sentimentjaモデルで分析
        '''
        analyzer = Analyzer()
        # dfはAWSで感情分析した結果である必要あり
        df_aws_neg = self.df
        # ネガティブのみ分析する
        df = df_aws_neg.copy()
        # テキストデータの抽出
        text_list = df.text.tolist()
        df = df.reset_index()
        sentiment_list = []
        # SJでの分析
        print('Running SJ Model\n')
        for i, text in enumerate(text_list):
            sentiment_list.append(analyzer([text]))
        print('Done SJ Model\n')
        # 感情の初期化
        df['happy'] = 0
        df['sad'] = 0
        df['disgust'] = 0
        df['angry'] = 0
        df['fear'] = 0
        df['surprise'] = 0
        # DataFrameに分析結果の挿入
        for i in range(len(df)):
            df['happy'][i] = np.round(10 * (sentiment_list[i][0].get('emotions').get('happy')) + 1, 3)
            df['sad'][i] = np.round(10 * (sentiment_list[i][0].get('emotions').get('sad')) + 1, 3)
            df['disgust'][i] = np.round(10 * (sentiment_list[i][0].get('emotions').get('disgust')) + 1, 3)
            df['angry'][i] =  np.round(10 * (sentiment_list[i][0].get('emotions').get('angry')) + 1, 3)
            df['surprise'][i] = np.round(10 * (sentiment_list[i][0].get('emotions').get('surprise')) + 1, 3)

        df = df[['tweet_id', 'day_id', 'created_at', 'user_friends', 'user_followers', 'retweet_count', 'favorite_count',
                 'AWS', 'happy', 'sad', 'disgust', 'angry', 'surprise', 'text']]

        return df





