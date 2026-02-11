from airflow import DAG
from airflow.decorators import task
import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.datasets import Dataset
from notify.discord import task_fail_callback
doc_md = """
    ### dag datahub ingestion for dbt 

    summary : datahub ingestion for market_price dbt projects.
"""

with DAG(
    dag_id= 'datahub_ingestion_dbt_market_price',
    start_date= pendulum.parse('2026-01-21',tz = 'Asia/Bangkok'),
    schedule= [Dataset(uri = 'x-market-price://ingestion')],
    catchup= False,
    tags= [
        'datahub_ingestion',
        'dbt',
        'MarketPrice'
    ],
    default_args={
        "owner" : "Phanu",
        "on_failure_callback" : lambda context : task_fail_callback(webhook_variable_key= "on_fail_webhook",context=context)
    },
    doc_md = doc_md
) as dag :
    
    run_ingestion = DockerOperator(
        task_id = 'run_ingestion',
        image = 'datahub-dbt-ingest:0.0.1',
        mounts = [
            Mount(source = "{{ var.value.process_market_price_dbt_mount_path }}/target", target= "/opt/target",type= 'bind' ),
            Mount(source = "{{ var.value.process_market_price_dbt_mount_path }}/recipe.yml", target= "/opt/recipe.yml", type= 'bind' )
        ],
        command= '/bin/bash -c "datahub ingest -c recipe.yml"',
        docker_url= 'tcp://docker-proxy:2375',
        network_mode='pipeline-network',
        mount_tmp_dir= False,
        auto_remove= 'force'
    )

    run_ingestion