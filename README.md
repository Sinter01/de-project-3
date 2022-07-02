# Проект 3
from sqlalchemy import create_engine
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
base_url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'
api_key = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
nickname = 'Sinter01'
cohort = '1'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

engine = create_engine('postgresql+psycopg2://jovyan:jovyan@localhost/de')
conn = engine.connect()

postgres_conn_id = 'postgresql_de'

def generate_report(ti):
    logging.info('requesting reports')
    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    logging.info(f'Response: {response.content}')


def get_report(ti):
    logging.info('get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    response = requests.get(
        f"https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id='MjAyMi0wNy0wMlQxNzoxMDozNwlTaW50ZXIwMQ=='",
        headers={
            "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
            "X-Nickname": "Sinter01",
            "X-Project": "True",
            "X-Cohort": "1"
        }
    )
    report_id = json.loads(response.content)['data']['report_id']

    ti.xcom_push(key='report_id', value=report_id)
    logging.info(f'Report_id={report_id}')


def get_files(filename, date, pg_table, ti):
    report_id = ti.xcom_pull(key='report_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-slogging.info3/cohort_{cohort}/{nickname}/project/{report_id}/{filename}'
    local_filename = '/lessons/' + date.replace('-', '') + '_' + filename
    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df.drop_duplicates(subset=['id'])

    df.to_sql(pg_table, engine, schema='staging', if_exists='append', index=False)



def insert_mart(path_to_mart):
    query = open(path_to_mart).read()
    conn.execute(query)



def get_increment(date, ti):
    logging.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    logging.info(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    logging.info(f'increment_id={increment_id}')



def get_increment_files(filename, date, pg_table, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-slogging.info3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df.drop_duplicates()

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema='staging', if_exists='append', index=False)


args = {
    "owner": "sinter",
    'email': ['semion.polonskii@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        default_args=args,
        description='ETL',
        catchup=False,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    user_order_log_cr = PostgresOperator(
        task_id='user_order_log_cr',
        postgres_conn_id=postgres_conn_id,
        sql='/migration/staging.user_order_log.sql')

    customer_research_cr = PostgresOperator(
        task_id='customer_research_cr',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/staging.customer_research.sql')

    user_activity_log_cr = PostgresOperator(
        task_id='user_activity_log_cr',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/user_activity_log.sql')

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)
    get_user_orders_log = PythonOperator(
        task_id='get_user_order_log',
        python_callable=get_files,
        op_kwargs={'filename': 'user_orders_log.csv',
                   'pg_table': 'user_order_log',
                   'date': business_dt})

    get_user_activity_log = PythonOperator(
        task_id='get_user_activity_log',
        python_callable=get_files,
        op_kwargs={'filename': 'user_activity_log.csv',
                   'pg_table': 'user_activity_log',
                   'date': business_dt})

    get_customer_research = PythonOperator(
        task_id='get_customer_research',
        python_callable=get_files,
        op_kwargs={'filename': 'customer_research.csv',
                   'pg_table': 'customer_research',
                   'date': business_dt})

    insert_mart_d_city = PostgresOperator(
        task_id='insert_d_city',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/mart.d_city.sql')

    insert_mart_d_customer = PostgresOperator(
        task_id='insert_d_customer',
        postgres_conn_id=postgres_conn_id,
        sqls='/migrations/mart.d_customer.sql')

    insert_mart_d_item = PostgresOperator(
        task_id='insert_d_item',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/mart.d_item.sql')

    insert_mart_f_sales = PostgresOperator(
        task_id='insert_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/mart.f_sales.sql')

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    get_user_orders_log_inc = PythonOperator(
        task_id='get_user_order_log_inc',
        python_callable=get_increment_files,
        op_kwargs={'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'date': business_dt})

    get_user_activity_log_inc = PythonOperator(
        task_id='get_user_activity_log_inc',
        python_callable=get_increment_files,
        op_kwargs={'filename': 'user_activity_log_inc.csv',
                   'pg_table': 'user_activity_log',
                   'date': business_dt})

    get_customer_research_inc = PythonOperator(
        task_id='get_customer_research_inc',
        python_callable=get_increment_files,
        op_kwargs={'filename': 'customer_research_inc.csv',
                   'pg_table': 'customer_research',
                   'date': business_dt})

    change_schema = PostgresOperator(
        task_id='change_schema',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/change_schema.sql')

    insert_mart_f_sales_new = PostgresOperator(
        task_id='insert_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql='/migrations/mart.f_sales_new.sql')

    (
            [user_order_log_cr, customer_research_cr, user_activity_log_cr] >>
            generate_report >>
            get_report >>
            [get_user_orders_log, get_user_activity_log, get_customer_research] >>
            insert_mart_d_city >>
            [insert_mart_d_customer, insert_mart_d_item] >>
            insert_mart_f_sales >>
            get_increment >>
            [get_user_orders_log_inc, get_user_activity_log_inc, get_customer_research_inc] >>
            change_schema >>
            get_increment >>
            [get_user_orders_log_inc, get_user_activity_log_inc, get_customer_research_inc] >>
            insert_mart_d_city >>
            [insert_mart_d_customer, insert_mart_d_item] >>
            insert_mart_f_sales_new
    )
