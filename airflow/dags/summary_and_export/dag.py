from airflow import DAG
from airflow.decorators import task
import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

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
            Mount(source='//c/Users/ASUS/OneDrive/learning/de_project/dbt-with-airflow/dbt/projects/process_market_price', target='/usr/app/dbt/process_market_price', type= 'bind')
        ],
        entrypoint= 'dbt',
        command= ["run","--project-dir","/usr/app/dbt/process_market_price","--profiles-dir","/usr/app/dbt/process_market_price"],
        docker_url= 'tcp://docker-proxy:2375',
        network_mode= 'pipeline-network',
        mount_tmp_dir=False,
        auto_remove='force'
    )

    @task()
    def end():
        pass

    start = start()
    end = end()

    start >> t1 >> end
