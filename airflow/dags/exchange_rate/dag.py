from airflow import DAG
from airflow.decorators import task
import pendulum 
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from datahub_airflow_plugin.entities import Dataset as DatasetDH
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from notify.discord import task_fail_callback
from airflow.datasets.metadata import Metadata
from airflow.datasets import Dataset

doc_md = """
    ### get exchnage rate dag
    summary : get exchange rate from [openexchange](https://openexchangerates.org/login?code=purple&redirect_to=account/usage) every hour and save to minio.  
    
    part of market_price project.
"""

exchange_dataset = Dataset(uri = 'x-market-price://exchange_rate')

with DAG(
    dag_id = 'get_exchange_rate',
    start_date= pendulum.parse('2025-10-14'),
    schedule= '0 * * * *',
    catchup = False,
    max_active_runs = 1,
    tags=['MarketPrice'],
    default_args= {
        'owner' : "Phanu",
        "on_failure_callback" : lambda context : task_fail_callback(webhook_variable_key= "on_fail_webhook",context=context)
    },
    doc_md = doc_md
) as dag:

    def check_status(response):
        try:
            response.raise_for_status()
            return True
        except Exception as e:
            return False

    t1 = S3CreateBucketOperator(
        task_id = 'create_bucket',
        bucket_name = 'exchange-rate',
        aws_conn_id = 'minio'
    )

    t2 = HttpOperator(
        task_id = 'get_exchange_rate_api',
        inlets = [
            DatasetDH(platform = 'RestAPI', name = 'openexchangerates API')
        ],
        http_conn_id= 'exchange_rate_api',
        endpoint='/latest.json',
        method='GET',
        headers = {"accept": "application/json"},
        data = {
            "app_id" : "{{ var.value.exchange_rate_api_key }}",
        },
        response_check= lambda response : check_status(response),
        response_filter= lambda response : response.json(),
        log_response = False,
        do_xcom_push = True,
    )

    @task(
        outlets=[exchange_dataset]
    )
    def save_exchange_rate():
        import pandas as pd
        import io
        context = get_current_context()
        ti = context.get('ti')
        df = pd.DataFrame([ti.xcom_pull(task_ids='get_exchange_rate_api')])
        df['create_at_bi'] = pendulum.now().to_datetime_string()
        hook = S3Hook(aws_conn_id = 'minio')

        file_name = f"exchange_rate_{pendulum.now().to_datetime_string().replace(' ','_')}.parquet"
        with io.BytesIO() as buffer:
                buffer.write(
                    bytes(
                        df.to_parquet(index=False)
                    )
                )
                hook.load_bytes(buffer.getvalue(),
                                bucket_name="exchange-rate",
                                key=file_name,
                                replace=True)
        
        yield Metadata(exchange_dataset, {"file_name": file_name})

    t3 = save_exchange_rate()
    [t1,t2] >> t3