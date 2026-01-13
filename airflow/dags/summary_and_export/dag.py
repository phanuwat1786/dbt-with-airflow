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


with DAG(
    dag_id = 'summary_and_export_v1',
    start_date= pendulum.parse('2026-01-08',tz = 'Asia/Bangkok'),
    schedule= None,
    catchup=False,
    max_active_runs= 1
) as dag :
    
    @task()
    def start():
        pass

    t1 = DockerOperator(
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
        command= "deps --project-dir /usr/app/dbt/process_market_price --profiles-dir /usr/app/dbt/process_market_price \&& build --project-dir /usr/app/dbt/process_market_price --profiles-dir /usr/app/dbt/process_market_price",
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
    t2 = get_data_to_export()
    t3 = export_to_ggs(data = t2)
    end = end()

    start >> t1 >> t2 >> t3 >> end