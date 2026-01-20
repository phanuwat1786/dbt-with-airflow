from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
import json
import pandas as pd
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from datahub_airflow_plugin.entities import Dataset as DatasetDH, Urn


with DAG(
    dag_id='bitcoin_price',
    start_date= pendulum.parse('2025-10-14',tz='Asia/Bangkok'),
    schedule='5 * * * *',
    catchup=False,
    max_active_runs = 1,
    tags=['MarketPrice'],
    owner_links={"Phanu","https://git.inet.co.th/phanuwat.su"}
) as dag:

    t1 = HttpOperator(
        task_id = 'get_bitcoin_price_from_api',
        http_conn_id= 'coindesk-bitcoin-price-api',
        method = 'GET',
        data= {"fsym":"BTC","tsyms":"USD,THB","api_key":"{{ var.value.coindesk_api_key }}"},
        headers={"Content-type":"application/json; charset=UTF-8"},
        response_filter = lambda response: json.loads(response.text),
        do_xcom_push = True,
        log_response = True,
        inlets = [
            DatasetDH(platform = 'RestAPI', name = 'coindeskAPI')
        ]
    )

    @task(
        outlets = [
            DatasetDH(platform = "postgres",name = "market_price.raw.bitcoin"),
            Dataset("x-market-price://bitcoin"),
        ],
    )
    def save_bitcoin_price():
        context = get_current_context()
        ti = context.get('ti')
        df = pd.DataFrame([ti.xcom_pull(task_ids='get_bitcoin_price_from_api')])
        df['create_at_bi'] = pendulum.now().to_datetime_string()
        pg_hook = PostgresHook(
            postgres_conn_id = 'pg_market_price'
        )
        df.to_sql(name = 'bitcoin',con = pg_hook.get_sqlalchemy_engine(),index = False,if_exists = 'append',schema='raw')

    t2 =  save_bitcoin_price()
    t1 >> t2