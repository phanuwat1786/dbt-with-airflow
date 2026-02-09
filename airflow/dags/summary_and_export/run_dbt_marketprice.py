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

doc_md = """
    ### run market_price dbt project
    summary : run dbt market_price dbt project after upstream dataset was updated.
    
    part of market_price project.
"""

with DAG(
    dag_id = 'run_dbt_market_price',
    start_date= pendulum.parse('2026-01-23',tz = 'Asia/Bangkok'),
    schedule= [Dataset(uri = 'x-market-price://bitcoin'), Dataset(uri = 'x-market-price://gold')],
    catchup=False,
    max_active_runs= 1,
    tags=['MarketPrice','DBT'],
    doc_md = doc_md,
    default_args= {
        'owner' : "Phanu"
    }
) as dag :

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

    t_check_source_freshness = DockerOperator(
        task_id = 'check_source_freshness',
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
        command= "source freshness --project-dir /usr/app/dbt/process_market_price --profiles-dir /usr/app/dbt/process_market_price",
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
        auto_remove='force',
        outlets = [
            Dataset(uri = 'x-market-price://ingestion'),
            Dataset(uri = 'x-market-price://fact-data')
        ]
    )

    t_generate_dbt_doc >> t_check_source_freshness >> t_run_dbt