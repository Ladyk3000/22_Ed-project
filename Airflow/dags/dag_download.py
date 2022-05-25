from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import os
from os import getenv
from sqlalchemy import create_engine

from datetime import datetime

# Google Drive downloads
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.utils.dates import days_ago

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "GOOGLE_DRIVE_DOWNLOAD"
schedule = "@hourly"

# Google drive environment variables
FOLDER_ID = os.environ.get("FOLDER_ID", "---")
CUSTOMERS_FILE_NAME=os.environ.get('CUSTOMERS_FILE_NAME', 'Customers.csv')
PRODUCTS_FILE_NAME=os.environ.get('PRODUCTS_FILE_NAME', 'products.csv')
PAYMENTS_FILE_NAME=os.environ.get('PAYMENTS_FILE_NAME', 'Payment.csv')
CHARGES_FILE_NAME=os.environ.get('CHARGES_FILE_NAME', 'charges.csv')
BUSINESS_PRODUCT_INSTANCES_FILE_NAME=os.environ.get('BUSINESS_PRODUCT_INSTANCES_FILE_NAME', 'business_product_instances.csv')
COSTED_EVENTS_FILE_NAME=os.environ.get('COSTED_EVENTS_FILE_NAME', 'costed_events_0.csv')

OUTPUT_CUSTOMERS_FILE_NAME=os.environ.get('OUTPUT_CUSTOMERS_FILE_NAME', 'customers.csv')
OUTPUT_PRODUCTS_FILE_NAME=os.environ.get('OUTPUT_PRODUCTS_FILE_NAME', 'products.csv')
OUTPUT_PAYMENTS_FILE_NAME=os.environ.get('OUTPUT_PAYMENTS_FILE_NAME', 'payments.csv')
OUTPUT_CHARGES_FILE_NAME=os.environ.get('OUTPUT_CHARGES_FILE_NAME', 'charges.csv')
OUTPUT_BUSINESS_PRODUCT_INSTANCES_FILE_NAME=os.environ.get('OUTPUT_BUSINESS_PRODUCT_INSTANCES_FILE_NAME', 'business_product_instances.csv')
OUTPUT_COSTED_EVENTS_FILE_NAME=os.environ.get('OUTPUT_COSTED_EVENTS_FILE_NAME', 'costed_events_0.csv')

def load_csv_pandas(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}" # The last one is the database
    df = pd.read_csv(file_path)
    engine = create_engine(jdbc_url)
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")


def datamart_pandas(table_name: str, schema: str = "datamart", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    engine = create_engine(jdbc_url)
    df = pd.read_sql("""
                    select c.customer_id, sum(p.amount) as amount, current_timestamp as execution_timestamp
                    from raw.customer as c
                    join raw.payments as p on c.customer_id=p.customer_id
                    group by c.customer_id
                    """,
                     engine)
    df.to_sql(table_name, engine, schema=schema, if_exists="append")

def get_next_to_load(table_name: str, schema: str = 'raw', conn_id: str = None) -> int:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    engine = create_engine(jdbc_url)
    does_exist = pd.read_sql("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE  table_schema = 'schema_name'
                        AND    table_name   = 'table_name'
                    ) AS does_exist;
                    """,
                     engine)['does_exist'][0]
    if does_exist:
        last_file_number = pd.read_sql("""
                            SELECT file_number
                            FROM costed_event
                            ORDER BY file_number DESC""",
                         engine)['file_number'][0]
        return last_file_number + 1
    else:
        return -1


with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         # default_args=DAG_DEFAULT_ARGS,
         start_date=days_ago(1),
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    # start_task = DummyOperator(task_id='START', dag=dag)

    customer_table_name = 'customer'
    product_table_name = 'product'
    business_product_instance_table_name = 'business_product_instance'
    payment_table_name = 'payment'
    charge_table_name = 'charge'
    costed_event_table_name = 'costed_event'

    datamart_table = "customers_promo"

    # Load customers
    download_customers = GoogleDriveToLocalOperator(
        task_id="download_customers",
        folder_id=FOLDER_ID,
        file_name=CUSTOMERS_FILE_NAME,
        output_file=f"{AIRFLOW_HOME}/raw/" + OUTPUT_CUSTOMERS_FILE_NAME,
    )

    load_customer_raw_task = PythonOperator(dag=dag,
        task_id=f"{DAG_ID}.RAW.{customer_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/raw/{OUTPUT_CUSTOMERS_FILE_NAME}",
            "table_name": customer_table_name,
            "conn_id": "raw_postgres"
        }
    )

    # Load products
    download_products = GoogleDriveToLocalOperator(
        task_id="download_products",
        folder_id=FOLDER_ID,
        file_name=PRODUCTS_FILE_NAME,
        output_file=f"{AIRFLOW_HOME}/raw/{OUTPUT_PRODUCTS_FILE_NAME}",
    )

    load_products_raw_task = PythonOperator(dag=dag,
        task_id=f"{DAG_ID}.RAW.{product_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/raw/{OUTPUT_PRODUCTS_FILE_NAME}",
            "table_name": product_table_name,
            "conn_id": "raw_postgres"
        }
    )

    # Load business product instances
    download_business_product_instances = GoogleDriveToLocalOperator(
        task_id="download_business_product_instances",
        folder_id=FOLDER_ID,
        file_name=BUSINESS_PRODUCT_INSTANCES_FILE_NAME,
        output_file=f"{AIRFLOW_HOME}/raw/{OUTPUT_BUSINESS_PRODUCT_INSTANCES_FILE_NAME}",
    )

    load_business_product_instances_raw_task = PythonOperator(dag=dag,
        task_id=f"{DAG_ID}.RAW.{business_product_instance_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/raw/{OUTPUT_BUSINESS_PRODUCT_INSTANCES_FILE_NAME}",
            "table_name": business_product_instance_table_name,
            "conn_id": "raw_postgres"
        }
    )

    # Load payments
    download_payments = GoogleDriveToLocalOperator(
        task_id="download_payments",
        folder_id=FOLDER_ID,
        file_name=PAYMENTS_FILE_NAME,
        output_file=f"{AIRFLOW_HOME}/raw/{OUTPUT_PAYMENTS_FILE_NAME}",
    )

    load_payments_raw_task = PythonOperator(dag=dag,
        task_id=f"{DAG_ID}.RAW.{payment_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/raw/{OUTPUT_PAYMENTS_FILE_NAME}",
            "table_name": payment_table_name,
            "conn_id": "raw_postgres"
        }
    )

    # Load charges
    download_charges = GoogleDriveToLocalOperator(
        task_id="download_charges",
        folder_id=FOLDER_ID,
        file_name=CHARGES_FILE_NAME,
        output_file=f"{AIRFLOW_HOME}/raw/{OUTPUT_CHARGES_FILE_NAME}",
    )

    load_charges_raw_task = PythonOperator(dag=dag,
        task_id=f"{DAG_ID}.RAW.{charge_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/raw/{OUTPUT_CHARGES_FILE_NAME}",
            "table_name": charge_table_name,
            "conn_id": "raw_postgres"
        }
    )

    # Load costed events
    detect_costed_event_file = GoogleDriveFileExistenceSensor(
        task_id="detect_costed_event_file", folder_id=FOLDER_ID, file_name=COSTED_EVENTS_FILE_NAME
    )

    download_costed_event = GoogleDriveToLocalOperator(
        task_id="download_costed_event",
        folder_id=FOLDER_ID,
        file_name=COSTED_EVENTS_FILE_NAME,
        output_file=f"{AIRFLOW_HOME}/raw/{OUTPUT_COSTED_EVENTS_FILE_NAME}",
    )

    load_costed_event_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{costed_event_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "file_path": f"{AIRFLOW_HOME}/raw/{OUTPUT_COSTED_EVENTS_FILE_NAME}",
                                                "table_name": costed_event_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )


    [download_customers >> load_customer_raw_task,
    download_products >> load_products_raw_task,
    download_business_product_instances >> load_business_product_instances_raw_task,
    download_payments >> load_payments_raw_task,
    download_charges >> load_charges_raw_task,
    download_costed_event >> load_costed_event_raw_task]
