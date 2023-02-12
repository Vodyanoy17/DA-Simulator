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
def a_adag_gv_71a():

    @task()
    def get_activitiess1d(connection=connection):

        act_sql = """select
          count(DISTINCT  user_id) as "DAU yesterday",
          countIf(user_id, action='like') as "likes",
          countIf(user_id, action='view') AS "views",
          round(countIf(user_id, action='like') / countIf(user_id, action='view'),2) AS "CTR"
        FROM
          {db}.feed_actions
        WHERE
          time >= yesterday()
          and time < today()
          """

        users_df = ph.read_clickhouse(act_sql, connection=connection).values[0]
        return users_df
    
    @task()
    def plot_activitiess7d(connection=connection):
        query = '''
        SELECT 
            toDate(time) as date,
            COUNT(DISTINCT user_id) as dau, 
            sum(action = 'like') as likes,
            sum(action = 'view') as views,
            likes/views as ctr
        FROM {db}.feed_actions
        WHERE date BETWEEN today()-7 AND today()
        GROUP BY date
        '''
        
        df = ph.read_clickhouse(query, connection=connection)
        
        plt.figure(figsize=(10, 6))
        sns.lineplot(x="date", y="likes", data=df, label='likes')
        sns.lineplot(x="date", y="views", data=df, label='views')
        sns.lineplot(x="date", y="dau", data=df, label='dau')
        sns.lineplot(x="date", y="ctr", data=df, label='ctr')
        plt.suptitle('Views and Likes for the Last 7 Days')
        plt.xticks(rotation=45)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        

    
    @task()
    def transform(order_data_dict):
        
        dau = order_data_dict[0]
        viwes = order_data_dict[1]
        likes = order_data_dict[2]
        ctr = order_data_dict[3]
        msg = f"Ключевых метрик за предыдущий день :\nDAU:{dau},\nПросмотры:{viwes},\nЛайки:{likes},\nCTR:{ctr}"

        bot.sendMessage(chat_id=chat_id, text=msg)
    
    yesterday = get_activitiess1d()
    transform(yesterday)
    plot_activitiess7d()

a_adag_gv_71 = a_adag_gv_71a()