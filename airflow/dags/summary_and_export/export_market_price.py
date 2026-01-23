from airflow import DAG
from airflow.decorators import task,task_group
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.models import Variable
from gspread_pandas import Spread
from google.oauth2 import service_account
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context
from datahub_airflow_plugin.entities import Dataset as DatasetDH

with DAG(
    dag_id = 'export_ggs_market_price',
    start_date= pendulum.parse('2026-01-23',tz = 'Asia/Bangkok'),
    schedule= [Dataset(uri = 'x-market-price://fact-data')],
    catchup=False,
    max_active_runs= 1,
    tags=['MarketPrice'],
    default_args= {
        'owner' : "Phanu"
    }
) as dag :

    @task
    def get_export_list():
        return Variable.get('market_price_export',deserialize_json=True)

    @task_group( )
    def export(export_table:dict):

        @task(
            map_index_template = "{{ table_name }}",
            inlets = [ DatasetDH(platform= "postgres",name = "market_price.raw", env = 'PROD') ]
            )
        def get_data_to_export(export_table: dict):

            table_name = export_table.get('table_name')
            context = get_current_context()

            context['table_name'] = table_name
            
            pg_hook = PostgresHook(
                postgres_conn_id = 'pg_market_price'
            )

            df = pd.read_sql(
                sql = f'SELECT * FROM fact.{table_name}',
                con = pg_hook.get_sqlalchemy_engine()
            )

            return df
        
        @task(
            map_index_template = "{{ worksheet_name }}",
            outlets = [
                  DatasetDH(platform="GoogleSheet",name = "MarketPrice")
              ]
            )
        def export_to_ggs(export_table: dict,data):

            worksheet_name = export_table.get('sheet_name')
            context = get_current_context()
            context["worksheet_name"] = worksheet_name

            credential = service_account.Credentials.from_service_account_info(
                info = Variable.get("cred_ggs_audit", deserialize_json=True),
                scopes = ['openid', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/spreadsheets']
            )

            sheet_name = 'MarketPrice'
            spread = Spread(spread = sheet_name, creds= credential )
            spread.open_sheet(sheet = worksheet_name, create= True )
            spread.df_to_sheet(data, replace=True, add_filter= False )

        t_get_data_to_export = get_data_to_export(export_table)
        t_export_to_ggs = export_to_ggs(export_table,t_get_data_to_export)

        t_get_data_to_export >> t_export_to_ggs

    t_get_export_list = get_export_list()
    tg_export = export.expand(export_table = t_get_export_list)
    


    t_get_export_list >> tg_export