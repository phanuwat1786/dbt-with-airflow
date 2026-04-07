from airflow import DAG
from airflow.decorators import task
import pendulum
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context
from datahub_airflow_plugin.entities import Dataset as DatasetDH
from notify.discord import task_fail_callback
import logging


doc_md = '''
      ### header
          summary : dag summary.
'''

exchange_dataset = Dataset(uri = 'x-market-price://exchange_rate')

with DAG (
    dag_id = 'test_exchnage_rate_sparkjob',
    start_date = pendulum.parse('2026-04-07',tz = 'Asia/Bangkok'),
    schedule = [exchange_dataset],
    catchup = False,
    default_args = {
        'owner' : 'dag owner',
        'on_failure_callback' : lambda context : task_fail_callback(webhook_variable_key= 'webhook',context=context)
}
) as dag:

    logger = logging.getLogger(__name__)

    @task
    def print_triggering_dataset_events(triggering_dataset_events=None):
        for dataset, dataset_list in triggering_dataset_events.items():
            print(dataset, dataset_list)
            print(dataset_list[0].source_dag_run.dag_id)

    t1 = print_triggering_dataset_events()
    t1