from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import random

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}
dag = DAG("data_ingestion", default_args=default_args, schedule_interval="*/5 * * * *", catchup=False)

def read_data(**context):
    files = [f for f in os.listdir("/opt/airflow/data/raw-data") if f.endswith(".csv")]
    if not files:
        print("No new files found.")
        return None
    file = random.choice(files)
    filepath = f"/opt/airflow/data/raw-data/{file}"
    df = pd.read_csv(filepath)
    context["task_instance"].xcom_push(key="filename", value=file)
    context["task_instance"].xcom_push(key="data", value=df.to_json())
    print(f"📥 Read file: {file}")
    return file

def validate_data(**context):
    data_json = context["task_instance"].xcom_pull(key="data", task_ids="read_data")
    df = pd.read_json(data_json)
# Handle your dataset’s actual column names
# “Experience Level” is categorical, not numeric, so just check for missing values
    valid_mask = df['Experience Level'].notna() & df['Location'].notna() & df['Required Skills'].notna()
    valid_rows = valid_mask.sum()
    invalid_rows = len(df) - valid_rows
    context["task_instance"].xcom_push(key="valid_rows", value=int(valid_rows))
    context["task_instance"].xcom_push(key="invalid_rows", value=int(invalid_rows))
    context["task_instance"].xcom_push(key="valid_mask", value=valid_mask.tolist())

def split_save_data(**context):
    data_json = context["task_instance"].xcom_pull(key="data", task_ids="read_data")
    valid_mask = context["task_instance"].xcom_pull(key="valid_mask", task_ids="validate_data")
    filename = context["task_instance"].xcom_pull(key="filename", task_ids="read_data")
    df = pd.read_json(data_json)
    good_df = df[[v for v in valid_mask]]
    bad_df = df[[not v for v in valid_mask]]
    if len(good_df) > 0:
        good_df.to_csv(f"/opt/airflow/data/good_data/{filename}", index=False)
    if len(bad_df) > 0:
        bad_df.to_csv(f"/opt/airflow/data/bad_data/{filename}", index=False)
    print(f"✅ Split {filename} → Good: {len(good_df)}, Bad: {len(bad_df)}")

PythonOperator(task_id="read_data", python_callable=read_data, dag=dag) >> \
PythonOperator(task_id="validate_data", python_callable=validate_data, dag=dag) >> \
PythonOperator(task_id="split_save_data", python_callable=split_save_data, dag=dag)
