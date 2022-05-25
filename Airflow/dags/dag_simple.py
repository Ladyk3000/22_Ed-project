from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine

from datetime import datetime

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "SIMPLE"
schedule = "@hourly"


def load_csv_pandas(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
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

    customer_table_name = "customer"
    payments_table_name = "payments"
    datamart_table = "customer_totals"

    load_customer_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{customer_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "file_path": f"{AIRFLOW_HOME}/example/customer.csv",
                                                "table_name": customer_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )

    load_payments_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "file_path": f"{AIRFLOW_HOME}/example/payments.csv",
                                                "table_name": payments_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )

    customer_totals_datamart_task = PythonOperator(dag=dag,
                                                   task_id=f"{DAG_ID}.DATAMART.{datamart_table}",
                                                   python_callable=datamart_pandas,
                                                   op_kwargs={
                                                       "table_name": datamart_table,
                                                       "conn_id": "datamart_postgres"
                                                   }
                                                   )
    start_task >> [load_customer_raw_task, load_payments_raw_task] >> customer_totals_datamart_task >> end_task

    # 1) DAG for raw layer, separated postgres
    # 2) DAG for datamart layer and dependency resolving
    # 3) Data from external source (e.g. google drive)
