from airflow import DAG
from airflow.decorators import task
import pendulum 
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.datasets import Dataset
from datahub_airflow_plugin.entities import Dataset as DatasetDH, Urn
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id = 'get_gold_price',
    start_date= pendulum.parse('2025-10-13',tz = 'Asia/Bangkok'),
    schedule='5 3,4,8 * * *',
    catchup=False,
    max_active_runs = 1
):
    def check_status(response):
        try:
            response.raise_for_status()
            return True
        except Exception as e:
            return False
    
    t1 = HttpOperator(
        task_id = 'get_gold_price_from_api',
        http_conn_id='gold_price_api',
        endpoint='/XAU/USD',
        headers={
        "x-access-token": "{{ var.value.goldprice_api_key }}",
        "Content-Type": "application/json"
        },
        method= 'GET',
        response_check = lambda response : check_status(response),
        response_filter= lambda response : response.json(),
        do_xcom_push = True,
        log_response=True,
        inlets = [
            DatasetDH(platform = 'RestAPI', name = 'goldAPI.io')
        ]
    )

    @task(
        outlets = [
            DatasetDH(platform = "postgres",name = "market_price.raw.gold",env = 'PROD'),
            Dataset(uri = 'x-market-price://gold')
        ]
    )
    def save_gold_price():
        import pandas as pd
        context = get_current_context()
        ti = context.get('ti')
        df = pd.DataFrame([ti.xcom_pull(task_ids='get_gold_price_from_api')])
        df['create_at_bi'] = pendulum.now().to_datetime_string()
        pg_hook = PostgresHook(
            postgres_conn_id = 'pg_market_price'
        )
        df.to_sql(name = 'gold',con = pg_hook.get_sqlalchemy_engine(),index = False,if_exists = 'append',schema='raw')

    t2 = save_gold_price()
    t1 >> t2