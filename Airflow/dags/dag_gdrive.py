from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
import requests
from datetime import datetime

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "LOAD_DATA_GDRIVE"
schedule = "@hourly"


def load_from_gdrive(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    #extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
 
    from google_drive_downloader import GoogleDriveDownloader as gdd
    try:
        gdd.download_file_from_google_drive(file_id='---',
        dest_path= f'{AIRFLOW_HOME}/example/Customers.csv',
        unzip=True)
    except:
        print('False')
    df = pd.read_csv(f'{AIRFLOW_HOME}/example/Customers.csv')
    columns = [
            'Customer_ID',
            'Date_of_birth', 
            'Gender', 
            'First name', 
            'Last name',   
            'MSISDN',
            'Customer_Email', 
            'Agree_for_promo',
            'Region',
            'Customer_City',
            'Customer_City_Type',
            'Category',
            'Customer_Since',
            'Status'
            ]
    new_df = df[columns].copy()
    engine = create_engine(jdbc_url)
    new_df.to_sql(table_name, engine, schema=schema, if_exists="replace")


with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    customer_table_name = "Full_customers"
   
    load_customers_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{customer_table_name}",
                                            python_callable=load_from_gdrive,
                                            op_kwargs={
                                                "file_path": f"{AIRFLOW_HOME}/example/Customers.csv",
                                                "table_name": customer_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )


    start_task >> load_customers_raw_task >> end_task