from airflow import DAG
from airflow.decorators import task
import pendulum 
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import get_current_context
from airflow.datasets import Dataset
from datahub_airflow_plugin.entities import Dataset as DatasetDH, Urn
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from airflow.models.taskinstance import TaskInstance
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
import logging

with DAG(
    dag_id = 'get_gold_price_v2',
    start_date= pendulum.parse('2026-01-26',tz = 'Asia/Bangkok'),
    schedule='5 * * * *',
    catchup=False,
    max_active_runs = 1,
    tags=['MarketPrice'],
    default_args= {
        'owner' : "Phanu"
    }
):
    
    logger = logging.getLogger(__name__)


    @task
    def reset_key_at_start_of_month(ti:TaskInstance):
        if pendulum.instance(ti.execution_date).day == 1:
            Variable.set(key = 'current_gold_api_number',  value = 1)
            logger.info("Start on month. Set number of API keys to 1.")

    @task(
        inlets = [
            DatasetDH(platform = 'RestAPI', name = 'goldAPI.io')
        ]
    )
    def get_gold_price_from_api(ti:TaskInstance ):
        
        http_hook = HttpHook(method= 'GET', http_conn_id= 'gold_price_api')

        
        token_number = int(Variable.get("current_gold_api_number",default_var= 1))

        while True:
            header = {
                "x-access-token": Variable.get(key = f'gold_price_api_key_{token_number}'),
                "Content-Type": "application/json",
            }

            response = http_hook.run(endpoint = '/XAU/USD', headers= header,extra_options={'check_response': False})

            if response.ok:
                Variable.set(key = "current_gold_api_number",value = token_number)
                return response.json()
            else:
                logger.info(f'response : {response.text}')
                if response.status_code == 403 and 'Monthly API quota exceeded' in response.text :
                    if token_number == 8:
                        logger.error("last api-key out of quota.")
                        response.raise_for_status()
                    else:
                        logger.info("using next api_key")
                        token_number += 1
                        continue
                else:
                    response.raise_for_status()

    @task(
        outlets = [
            DatasetDH(platform = "postgres",name = "market_price.raw.gold",env = 'PROD'),
            Dataset(uri = 'x-market-price://gold')
        ]
    )
    def save_gold_price(data:dict):
        import pandas as pd
        context = get_current_context()
        ti = context.get('ti')
        df = pd.DataFrame([ti.xcom_pull(task_ids='get_gold_price_from_api')])
        df['create_at_bi'] = ti.execution_date
        pg_hook = PostgresHook(
            postgres_conn_id = 'pg_market_price'
        )
        df.to_sql(name = 'gold',con = pg_hook.get_sqlalchemy_engine(),index = False,if_exists = 'append',schema='raw')

    t_reset_key_at_start_of_month = reset_key_at_start_of_month()
    t_get_gold_price_from_api = get_gold_price_from_api()
    t_save_gold_price = save_gold_price(data = t_get_gold_price_from_api)

    t_reset_key_at_start_of_month >> t_get_gold_price_from_api >> t_save_gold_price 