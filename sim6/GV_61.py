import pandas as pd
import datetime as dt
import pandahouse as ph
from datetime import datetime, timedelta


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


#  колоноки финальной таблицы
final_columns = [
    'views', 'likes',
    'messages_received', 'messages_sent','users_received','users_sent'
]

# Запрос для выгрузки данных из базы данных
QUERY_FEED = """
SELECT 
    toDate(time) AS event_date, 
    user_id,
    gender, age, os,
    countIf(action='view') AS views,
    countIf(action='like') AS likes
FROM {db}.feed_actions 
WHERE event_date = yesterday()
GROUP BY event_date, user_id, gender, age, os
"""

QUERY_MESSENGER = """
select event_date,user_id,age,os,gender,messages_sent,messages_received,
    users_received,users_sent from 
(SELECT 
    toDate(time) AS event_date, 
    user_id AS user_id,
    gender, age, os,
    count(*) AS messages_sent,
    uniq(reciever_id) AS users_sent
FROM {db}.message_actions
WHERE event_date = yesterday()
GROUP BY event_date, user_id, gender, age, os) as t1
FULL OUTER JOIN 
(select count(*) as messages_received, event_date,user_id,age,gender,os,uniq(user_id) AS users_received
from
(SELECT toDate(t1.time) AS event_date, 
    t1.reciever_id AS user_id,
    t2.age as age, t2.gender as gender ,t2.os as os
FROM {db}.message_actions t1
LEFT JOIN
(select distinct user_id,gender,age,os from {db}.message_actions) as t2
on t1.reciever_id=t2.user_id
WHERE event_date = yesterday() 
) as t3
GROUP BY event_date,user_id,age,gender,os) as t2
using user_id
"""

# новая таблица
CREATE_TABLE_TEST = """
CREATE TABLE IF NOT EXISTS test.etl_table_GV (
    event_date Date,
    dimension String,
    dimension_value String,
    views UInt64,
    likes UInt64,
    messages_received UInt64,
    messages_sent UInt64,
    users_received UInt64,
    users_sent UInt64
)
ENGINE = MergeTree()
ORDER BY event_date
"""


# In[ ]:



# Параметры подключения к базам данных
connection_simulator_20230120 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20230120',
    'user':'student',
    'password':'dpo_python_2020'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw',
    'password':'656e2b0c9c'
}


default_args = {
    'owner': 'gvinopal',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5),
    'start_date': dt.datetime(2023, 1, 1),
}

schedule_interval = '0 23 * * *'

# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_adag_gv_61():

    # Функция для получения датафрейма из базы данных Clickhouse
    @task
    def fetch_fd_from_clickhouse(query='SELECT 1', connection=connection_simulator_20230120):
        df = ph.read_clickhouse(query, connection=connection)
        return df


    # Функция для загрузки датафрейма в базу данных Clickhouse
    def insert_df_to_clickhouse(df, query='SELECT 1',connection=connection_test):
        ph.execute(query=CREATE_TABLE_TEST, connection=connection)
        ph.to_clickhouse(
            df, 'etl_table_GV',
            connection=connection, index=False
        )


    # объединение двух таблиц в одну
    @task
    def merge(df_feed, df_messenger):
        df_full = df_feed.merge(
            df_messenger,
            how='outer',
            on=['event_date', 'user_id', 'gender', 'age', 'os']
            ).dropna()
        df_full[final_columns] = df_full[final_columns].astype(int)
        return df_full
    
    #  преобразование данных по срезам
    @task
    def transform(df, metric):
        gpoup_by = ['event_date', metric]
        columns = gpoup_by + final_columns
        df_transform = df[columns]         .groupby(gpoup_by,as_index=False)         .sum() 
        return df_transform

    # Функция для сохранения данных в таблицу
    @task
    def save_data_to_db(df_gender, df_age, df_os):
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace = True)
        df_gender.insert(1, 'dimension', 'gender')

        df_age.rename(columns={'age': 'dimension_value'}, inplace = True)
        df_age.insert(1, 'dimension', 'age')

        df_os.rename(columns={'os': 'dimension_value'}, inplace = True)
        df_os.insert(1, 'dimension', 'os')

        df_final = pd.concat([df_gender, df_age, df_os])
        print(df_final.head(),CREATE_TABLE_TEST)

        insert_df_to_clickhouse(df_final, CREATE_TABLE_TEST)

    df_feed = fetch_fd_from_clickhouse(QUERY_FEED)
    df_messenger = fetch_fd_from_clickhouse(query=QUERY_MESSENGER)
    df_full = merge(df_feed, df_messenger)

    df_gender = transform(df_full, 'gender')
    df_age = transform(df_full, 'age')
    df_os = transform(df_full, 'os')

    save_data_to_db(df_gender, df_age, df_os)

#run everithing
a_adag_gv_61 = a_adag_gv_61()


