from airflow import DAG
from airflow.decorators import task
import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.models import Variable
from gspread_pandas import Spread
from google.oauth2 import service_account
from airflow.datasets import Dataset

with DAG(
    dag_id = 'summary_and_export_v1',
    start_date= pendulum.parse('2026-01-08',tz = 'Asia/Bangkok'),
    schedule= [Dataset(uri = 'x-market-price://bitcoin'), Dataset(uri = 'x-market-price://gold')],
    catchup=False,
    max_active_runs= 1,
    tags=['MarketPrice'],
    default_args= {
        'owner' : "Phanu"
    }
) as dag :
    
    @task()
    def start():
        pass

    t_generate_dbt_doc = DockerOperator(
        task_id = 'generate_dbt_docs',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.0',
        mounts= [
            Mount(source="{{ var.value.process_market_price_dbt_mount_path }}", target='/usr/app/dbt/process_market_price', type= 'bind')
        ],
        environment= {
            'DBT_DBNAME' : "{{ conn.pg_market_price.schema }}",
            'DBT_USER' : "{{ conn.pg_market_price.login }}",
            'DBT_HOST' : "{{ conn.pg_market_price.host }}",
            'DBT_PASS' : "{{ conn.pg_market_price.password }}",
            'DBT_PORT' : "{{ conn.pg_market_price.port }}"
        },
        entrypoint= 'dbt',
        command= "docs generate --project-dir /usr/app/dbt/process_market_price --profiles-dir /usr/app/dbt/process_market_price",
        docker_url= 'tcp://docker-proxy:2375',
        network_mode= 'pipeline-network',
        mount_tmp_dir=False,
        auto_remove='force'
    )

    t_run_dbt = DockerOperator(
        task_id = 'run_dbt',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.0',
        mounts= [
            Mount(source="{{ var.value.process_market_price_dbt_mount_path }}", target='/usr/app/dbt/process_market_price', type= 'bind')
        ],
        environment= {
            'DBT_DBNAME' : "{{ conn.pg_market_price.schema }}",
            'DBT_USER' : "{{ conn.pg_market_price.login }}",
            'DBT_HOST' : "{{ conn.pg_market_price.host }}",
            'DBT_PASS' : "{{ conn.pg_market_price.password }}",
            'DBT_PORT' : "{{ conn.pg_market_price.port }}"
        },
        entrypoint= 'dbt',
        command= "build --project-dir /usr/app/dbt/process_market_price --profiles-dir /usr/app/dbt/process_market_price",
        docker_url= 'tcp://docker-proxy:2375',
        network_mode= 'pipeline-network',
        mount_tmp_dir=False,
        auto_remove='force'
    )

    @task()
    def get_data_to_export():
        pg_hook = PostgresHook(
            postgres_conn_id = 'pg_market_price'
        )

        df = pd.read_sql(
            sql = f'SELECT * FROM fact.btc_and_gold_price',
            con = pg_hook.get_sqlalchemy_engine()
        )

        return df

    @task()
    def export_to_ggs(data):

        credential = service_account.Credentials.from_service_account_info(
            info = Variable.get("cred_ggs_audit", deserialize_json=True),
            scopes = ['openid', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/spreadsheets']
        )

        sheet_name = 'MarketPrice'
        spread = Spread(spread = sheet_name,creds= credential)
        spread.open_sheet('bitcoin_and_gold_price',create=True)
        spread.df_to_sheet(data,replace=True,add_filter=False)

    @task()
    def end():
        pass

    start = start()
    t_get_data_to_export = get_data_to_export()
    t_export_to_ggs = export_to_ggs(data = t_get_data_to_export)
    end = end()

    start >> t_generate_dbt_doc >> t_run_dbt >> end
    t_run_dbt >> t_get_data_to_export >> t_export_to_ggs >> end