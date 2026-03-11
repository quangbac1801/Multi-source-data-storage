from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import random

def crawl_data():
    return random.randint(1, 6)

with DAG(
    dag_id='etl1',
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    laydata = PythonOperator(
        task_id='crawl_data',
        python_callable=crawl_data
    )

    xldata = PythonOperator(
        task_id='xl_data',
        python_callable=crawl_data
    )
laydata >> xldata