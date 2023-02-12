import io
import telegram
import numpy as np
import pandas as pd
import datetime as dt
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20230120',
    'user':'student',
    'password':'dpo_python_2020'
}

my_token='5879848815:AAH5phGZXA5QDXXv6H7TtFLdZ48-oILXI90'
chat_id = 534065724
bot = telegram.Bot(token=my_token)

default_args = {
    'owner': 'gvinopal',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5),
    'start_date': dt.datetime(2023, 1, 1),
    'tags':['gv']
}

schedule_interval = '0 11 * * *'

# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_adag_gv_72():

    @task()
    def get_activitiess_today(connection=connection):
        query="""SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
               count(DISTINCT user_id) AS "users" ,
               countIf( action = 'like') AS "likes"
        FROM simulator_20230120.feed_actions
        GROUP BY toStartOfDay(toDateTime(time))
        order by __timestamp desc 
        limit 2
        """

        df = ph.read_clickhouse(query, connection=connection)
        ratio_users = round((1 - (1.0*df['users'][0])/df['users'][1])*100,2)
        ratio_likes = round((1 - (1.0 *df['likes'][0])/df['likes'][1])*100,2)
               
        msg1 = f"лайки сегодня:{df['likes'][0]}, оnотносительно вчера:{ratio_likes}%\n\
активные пользователи сегодня:{df['users'][0]}, оnотносительно вчера:{ratio_users}%"

        bot.sendMessage(chat_id=chat_id, text=msg1)
    
    @task()
    def plot_trafic_by_source(connection=connection):
        query = '''
        SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
               appl AS appl,
               source AS source,
               count(DISTINCT user_id) AS "cnt" 
        FROM
          (select user_id,
                  time,
                  source,
                  'msg' as appl
           FROM simulator_20230120.message_actions
           UNION all SELECT user_id,
                            time,
                            source,
                            'feed'
           FROM simulator_20230120.feed_actions AS T2) AS virtual_table
        GROUP BY appl,
                 source,
                 toStartOfDay(toDateTime(time))
        ORDER BY "cnt" DESC
        LIMIT 1000
           '''
       
        df = ph.read_clickhouse(query, connection=connection)
     
        plt.figure(figsize=(10, 6))
        sns.lineplot(x="__timestamp", y="cnt", data=df[(df['appl'] == 'feed') & (df['source'] == 'organic')],label='feed organic')
        sns.lineplot(x="__timestamp", y="cnt", data=df[(df['appl'] == 'feed') & (df['source'] == 'ads')],label='feed ads')
        sns.lineplot(x="__timestamp", y="cnt", data=df[(df['appl'] == 'msg') & (df['source'] == 'organic')],label='msg organic')
        sns.lineplot(x="__timestamp", y="cnt", data=df[(df['appl'] == 'msg') & (df['source'] == 'ads')],label='feed ads')
        plt.suptitle('Источник трафика лента vs . мессенджер')
        plt.xticks(rotation=45)        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'trafic_by_source_plot.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    @task()
    def plot_activities_today_vs(connection=connection):
    
        query = """select t.*, RANK() OVER (PARTITION BY cat ORDER BY date) as ranc
        from (
        SELECT toDateTime(intDiv(toUInt32(toDateTime(time)), 600)*600) AS date,
               count(user_id) AS event, '7d' as cat
        FROM simulator_20230120.feed_actions
        WHERE date BETWEEN today()-7 AND today()-6
        GROUP BY toDateTime(intDiv(toUInt32(toDateTime(time)), 600)*600)

        union all
        SELECT toDateTime(intDiv(toUInt32(toDateTime(time)), 600)*600) AS date,
               count(user_id) AS event, '1d'
        FROM simulator_20230120.feed_actions
        WHERE date  >= yesterday() AND date < today()
        GROUP BY toDateTime(intDiv(toUInt32(toDateTime(time)), 600)*600)

        union all
        SELECT toDateTime(intDiv(toUInt32(toDateTime(time)), 600)*600) AS date,
               count(user_id) AS event, 'today'
        FROM simulator_20230120.feed_actions
        WHERE date >=  today()  
        GROUP BY toDateTime(intDiv(toUInt32(toDateTime(time)), 600)*600)
        ) as t
        """

        df = ph.read_clickhouse(query, connection=connection)

        plt.figure(figsize=(20, 6))
        sns.lineplot(x="ranc", y="event", data=df[(df['cat'] == '7d')],label='события неделю тому')
        sns.lineplot(x="ranc", y="event", data=df[(df['cat'] == '1d') ],label='события вчера')
        sns.lineplot(x="ranc", y="event", data=df[(df['cat'] == 'today')],label='события сегодня')

        plt.suptitle('Все события сегодня/вчера/неделю тому')
        plt.xticks(rotation=45)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'events_by_date_plot.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    get_activitiess_today()
    plot_activities_today_vs()
    plot_trafic_by_source()

a_adag_gv_72 = a_adag_gv_72()