from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

def ingest_data():
    """Simulate ingestion of raw data and save to /tmp folder"""
    data = {
        "name": ["Ali", "Sara", "Bilal", "Aisha"],
        "age": [25, 28, 22, 30],
        "city": ["Lahore", "Karachi", "Islamabad", "Paris"]
    }
    df = pd.DataFrame(data)
    os.makedirs("/tmp/airflow_data", exist_ok=True)
    df.to_csv("/tmp/airflow_data/raw_data.csv", index=False)
    print("✅ Data ingested successfully!")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingest_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data_ingestion"]
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_data_task",
        python_callable=ingest_data,
    )

    ingest_task
