import dash
import dash as html
import dash as dcc
from dash import dash_table
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import datetime
import mysql.connector
import os,io

#==================================================================================================
# Fill out your info about TwitterAPI and AWS(if you want to run it on EC2)
config = {***}

mysql_info = {'DB_HOST':config['DB_HOST'] , 'DB_NAME': config['DB_NAME'],
                'DB_USER': config['DB_USER'], 'DB_PASSWORD': config['DB_PASSWORD'] , 'CHARSET': config['CHARSET']}

mydb = mysql.connector.connect(host=mysql_info['DB_HOST'],port=3306,
                                    db=mysql_info['DB_NAME'],user=mysql_info['DB_USER'],
                                    passwd=mysql_info['DB_PASSWORD'],charset=mysql_info['CHARSET'])

sql = "SELECT * FROM all_data"
cursor = mydb.cursor()
cursor.execute(sql)
rows = cursor.fetchall()
mydb.close()
cursor.close()

df = pd.DataFrame(list(rows))
columns = {0: 'tweet_id', 1: 'day_id', 2:'text', 3:'created_at', 4:'user_friends',
          5:'user_followers', 6:'retweet_count', 7:'favorite_count', 8:'AWS', 9:'happy',
          10:'sad', 11:'disgust', 12:'angry', 13:'surprise', 14:'day', 15:'risk'}
df = df.rename(columns = columns)

#==================================================================================================

#=================================================================================================================
#全体データ
#df = pd.read_csv('tweet_data.csv')
today = datetime.date.today()
yesterday = today - datetime.timedelta(1)

day_yesterday = df[df['day_id'].dt.date == yesterday]
day_today = df[df['day_id'].dt.date == today]

day_yesterday = day_yesterday.copy()
day_today = day_today.copy()

#datetime型に変換
day_yesterday['day_id'] = pd.to_datetime(day_yesterday['day_id'])
day_today['day_id'] = pd.to_datetime(day_today['day_id'])

#本日の総ツイート数
day_yesterday_TweetCou = len(day_yesterday)
day_today_TweetCou = len(day_today)

#本日の総ツイート人数
day_yesterday_TweetNum = len(day_yesterday.groupby(['tweet_id']).count())
day_today_TweetNum = len(day_today.groupby(['tweet_id']).count())

#本日の最高リツイート数
day_yesterday_ReTweetMax = day_yesterday['retweet_count'].max()
day_today_ReTweetMax = day_today['retweet_count'].max()

#本日の最高ファボ数
day_yesterday_FavMax = day_yesterday['favorite_count'].max()
day_today_FavMax = day_today['favorite_count'].max()

#tweetポジネガグラフ
#ポジネガ判定（データのそれぞれのcountについて）
day_today_po = day_today['AWS'][day_today['AWS'] == 'NEGATIVE'].count()
day_today_ne = day_today['AWS'][day_today['AWS'] == 'POSITIVE'].count()
colors = ['orange', '#dd1e35']
fig1 = go.Figure()
fig1.add_trace(go.Pie(labels=['POSITIVE', 'NEGATIVE'],
                     values=[day_today_po,day_today_ne],
                     marker=dict(colors=colors),
                     hoverinfo='label+value+percent',
                     textinfo='label+value',
                     #hole=7,
                     #rotation=45
                    ))

#=================================================================================================================
layout = go.Layout(
            title={'text': 'Total Cases: ',
                   'y': 0.93,
                   'x': 0.5,
                   'xanchor': 'center',
                   'yanchor': 'top'},
            titlefont={'color': 'white',
                       'size': 20},
            font=dict(family='sans-serif',
                      color='white',
                      size=12),
            hovermode='closest',
            paper_bgcolor='#1f2c56',
            plot_bgcolor='#1f2c56',
            legend={'orientation': 'h',
                    'bgcolor': '#1f2c56',
                    'xanchor': 'center', 'x': 0.5, 'y': -0.7}


        )
data1 = go.Pie(labels=['NEGATIVE', 'POSITIVE'],
                     values=[day_today_po,day_today_ne],
                     marker=dict(colors=colors),
                     hoverinfo='label+value+percent',
                     textinfo='label+value',
                     #hole=7,
                     #rotation=45
                    )
fig1 = go.Figure(data=data1, layout=layout)

df['day_id'] = pd.to_datetime(df['day_id'])
df_co = df.groupby('day_id', as_index=False).count()

data2 = go.Bar(x=df_co['day_id'],
               y=df_co['text'],
               name='Tweet推移',
               marker=dict(color='orange'),
               hoverinfo='text')
fig2 = go.Figure(data=data2, layout=layout)

#=================================================================================================================

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

rank = day_today.sort_values('risk', ascending=False)[:5]
rank = rank[['text', 'risk']]

#=================================================================================================================

app = dash.Dash(__name__, title='Falminf App',
assets_folder='static',
assets_url_path='static'
,meta_tags=[{"name": "viewport", "content": "width=device-width"}])

app.layout = html.Div([
    html.Div([
        html.Div([
            html.Img(src=app.get_asset_url('log3.jpg'),
                     id = 'corona-image',
                     style={'height': '60px',
                            'width': 'auto',
                            'margin-bottom': '25px'})

        ], className='one-third column'),

        html.Div([
            html.Div([
                html.H3('学研SNS動向', style={'margin-bottom': '0px', 'color': 'white'}),
                html.H5('炎上リスク監視', style={'margin-bottom': '0px', 'color': 'white'})
            ])
        ], className='one-half column', id = 'title'),


        html.Div([
            html.H6('Last Updated: ' + str(day_today['day_id'].iloc[1].strftime('%B, %d, %Y')) ,
                    style={'color': 'orange'})
        ], className='one-third column', id = 'title1')


    ], id = 'header', className= 'row flex-display', style={'margin-bottom': '25px'}),

    html.Div([
        html.Div([
            html.H6(children='本日のツイート数',
                    style={'textAlign': 'center',
                           'color': 'white'}),
            html.P(f"{day_today_TweetCou:,.0f}",
                   style={'textAlign': 'center',
                          'color': 'orange',
                          'fontSize': 40}),
            html.P('new: ' + f"{day_today_TweetCou - day_yesterday_TweetCou:,.0f}"
                   + ' (' + str(round(((day_today_TweetCou - day_yesterday_TweetCou) /
                                       (day_today_TweetCou)) *100, 2)) + '%)',
                   style={'textAlign': 'center',
                          'color': 'orange',
                          'fontsize': 15,
                          'margin-top': '-18px'})

        ], className='card_container three columns'),

html.Div([
            html.H6(children='ツイート人数',
                    style={'textAlign': 'center',
                           'color': 'white'}),
            html.P(f"{day_today_TweetNum:,.0f}",
                    style={'textAlign': 'center',
                           'color': '#dd1e35',
                           'fontSize': 40}),
            html.P('new: ' + f"{day_today_TweetNum - day_yesterday_TweetNum:,.0f}"
                   + ' (' + str(round(((day_today_TweetNum - day_yesterday_TweetNum) /
                                   day_today_TweetNum) * 100, 2)) + '%)',
                   style={'textAlign': 'center',
                          'color': '#dd1e35',
                          'fontSize': 15,
                          'margin-top': '-18px'})

        ], className='card_container three columns'),

html.Div([
            html.H6(children='最高リツート数',
                    style={'textAlign': 'center',
                           'color': 'white'}),
            html.P(f"{day_today_ReTweetMax:,.0f}",
                    style={'textAlign': 'center',
                           'color': 'green',
                           'fontSize': 40}),
            html.P('new: ' + f"{day_today_ReTweetMax - day_yesterday_ReTweetMax:,.0f}"
                   + ' (' + str(round(((day_today_ReTweetMax - day_yesterday_ReTweetMax) /
                                   day_today_ReTweetMax) * 100, 2)) + '%)',
                   style={'textAlign': 'center',
                          'color': 'green',
                          'fontSize': 15,
                          'margin-top': '-18px'})

        ], className='card_container three columns'),

html.Div([
            html.H6(children='最高ファボ数',
                    style={'textAlign': 'center',
                           'color': 'white'}),
            html.P(f"{day_today_FavMax:,.0f}",
                    style={'textAlign': 'center',
                           'color': '#e55467',
                           'fontSize': 40}),
            html.P('new: ' + f"{day_today_FavMax - day_yesterday_FavMax:,.0f}"
                   + ' (' + str(round(((day_today_FavMax - day_yesterday_FavMax) /
                                   day_today_FavMax) * 100, 2)) + '%)',
                   style={'textAlign': 'center',
                          'color': '#e55467',
                          'fontSize': 15,
                          'margin-top': '-18px'})

        ], className='card_container three columns'),

    ], className='row flex display'),


##select countryに関して詳細に記載する
##select countryのデザイン（style.css）の詳細に関して記載する．

    html.Div([
        html.Div([
dcc.Graph(id = 'pie_chart',
          figure=fig1,
          config={'displayModeBar': 'hover'},
          style={'background-color': 'rgba(255,0,0,0.5)'})
        ], className='create_container four columns'),

html.Div([
dcc.Graph(id = 'line_chart',
          figure=fig2,
          config={'displayModeBar': 'hover'})
        ], className='create_container five columns'),

    ],className='row flex-display'),

#html.Div(children=[
#    html.H6(children='リスク評価ランキング',
#           style={'textAlign': 'center','color': 'white'}),
#    generate_table(day4)
#])
    dash_table.DataTable(
        id='table',
        columns=[ {"name": i, "id": i} for i in rank.columns],
        data=rank.to_dict("rows")
    )

], id = 'mainContainer',style={'display': 'flex', 'flex-direction': 'column'})

server = app.server

if __name__ == '__main__':
    app.run_server(debug=True)





