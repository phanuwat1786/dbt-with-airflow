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
import logging
from notify.discord import DiscordNotify
from discord_webhook import DiscordEmbed
from airflow.exceptions import AirflowSkipException

doc_md = """
    ### export Market price data dag
    summary : export fact table to googlsheet as a data source for looker studio dashboard.
    
    part of market_price project.
"""

with DAG(
    dag_id = 'export_ggs_market_price',
    start_date= pendulum.parse('2026-01-23',tz = 'Asia/Bangkok'),
    schedule= [Dataset(uri = 'x-market-price://fact-data')],
    catchup=False,
    max_active_runs= 1,
    tags=['MarketPrice'],
    doc_md = doc_md,
    default_args= {
        'owner' : "Phanu"
    }
) as dag :

    logger = logging.getLogger(__name__)

    @task
    def get_export_list():
        return Variable.get('market_price_export',deserialize_json=True)

    @task_group()
    def export(export_table:dict):

        @task(
            map_index_template = "{{ table_name }}"
            )
        def get_data_to_export(export_table: dict):

            table_name = export_table.get('table_name')
            sheet_name = export_table.get('sheet_name')
            context = get_current_context()

            context['table_name'] = table_name

            ti = context['ti']
            
            t_name_list = [ item.get('table_name') for item in ti.xcom_pull(task_ids= 'get_export_list') ]
            context['ti'].task.inlets = [DatasetDH(platform= "postgres",name = f"market_price.fact.{t_name}",env = "PROD") for t_name in t_name_list ]  
            logger.info(ti.task.inlets)
            pg_hook = PostgresHook(
                postgres_conn_id = 'pg_market_price'
            )

            df = pd.read_sql(
                sql = f'SELECT * FROM fact.{table_name}',
                con = pg_hook.get_sqlalchemy_engine()
            )

            return { "table_name" : table_name,
                     "sheet_name" : sheet_name,
                     "df" : df }
        
        @task(
            map_index_template = "{{ worksheet_name }}",
            outlets = [
                  DatasetDH(platform="GoogleSheet",name = "Google Sheet 'MarketPrice'")
              ]
            )
        def export_to_ggs(data:dict):

            worksheet_name = data.get('sheet_name')
            df = data.get("df")
            context = get_current_context()
            context["worksheet_name"] = worksheet_name

            credential = service_account.Credentials.from_service_account_info(
                info = Variable.get("cred_ggs_audit", deserialize_json=True),
                scopes = ['openid', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/spreadsheets']
            )

            sheet_name = 'MarketPrice'
            spread = Spread(spread = sheet_name, creds= credential )
            spread.open_sheet(sheet = worksheet_name, create= True )
            spread.df_to_sheet(df, replace=True, add_filter= False )
            
        @task(map_index_template = "{{ table_name }}")
        def send_message_to_discord(data:dict):

            table_name = data.get('table_name')
            df = data.get('df')
            context = get_current_context()
            context["table_name"] = table_name
            if table_name not in [ item.get('table_name') for item in Variable.get('market_price_discord_table_condition',deserialize_json= True) ]:
                raise AirflowSkipException(f"skipping {table_name}.")
            
            wh = DiscordNotify(webhook=Variable.get('phanu_discord_webhook'),username= 'GOLD PRICE INFO')
            embed = DiscordEmbed(title = 'Hourly Gold Price Report', color = df['hr_font_color'].iloc[0].replace('#',''), url = Variable.get('market price dashboard link'))
            embed.add_embed_field(name = 'Price now', value= f'$ {df['current_price'].iloc[0]} USD')
            embed.add_embed_field(name = '1hr-change', value = f'{df['one-hr-change'].iloc[0]} %')
            embed.add_embed_field(name = '1day-change', value = f'{df['one-day-change'].iloc[0]} %')
            embed.set_timestamp()

            wh.send_embeded(embed = embed, extra_content= f'<@{Variable.get(key ='phanu_discord_user_id')}>')

        t_get_data_to_export = get_data_to_export(export_table= export_table)
        t_export_to_ggs = export_to_ggs(data= t_get_data_to_export)
        t_send_message_to_discord = send_message_to_discord(data = t_get_data_to_export)

        t_get_data_to_export >> [t_export_to_ggs,t_send_message_to_discord] 

    t_get_export_list = get_export_list()
    tg_export = export.expand(export_table = t_get_export_list)

    t_get_export_list >> tg_export