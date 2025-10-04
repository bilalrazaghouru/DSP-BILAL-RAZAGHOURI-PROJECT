from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os, shutil, random

RAW = "raw-data"
GOOD = "good-data"

def read_data():
    files = os.listdir(RAW)
    return os.path.join(RAW, random.choice(files)) if files else None

def save_file(ti):
    file_path = ti.xcom_pull(task_ids="read-data")
    if file_path:
        shutil.move(file_path, os.path.join(GOOD, os.path.basename(file_path)))
        print(f"Moved {file_path} → {GOOD}")

with DAG(
    dag_id="ingest_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="read-data",
        python_callable=read_data
    )

    t2 = PythonOperator(
        task_id="save-file",
        python_callable=save_file
    )

    t1 >> t2
