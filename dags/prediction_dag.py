from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os

def predict():
    """Simulate predictions from ingested data"""
    input_path = "/tmp/airflow_data/raw_data.csv"
    output_path = "/tmp/airflow_data/predictions.csv"

    if not os.path.exists(input_path):
        raise FileNotFoundError("❌ No input data found. Run ingest_dag first!")

    df = pd.read_csv(input_path)
    df["prediction"] = [random.choice(["Yes", "No"]) for _ in range(len(df))]
    df.to_csv(output_path, index=False)
    print("✅ Predictions generated successfully!")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="prediction_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["prediction"]
) as dag:

    prediction_task = PythonOperator(
        task_id="prediction_task",
        python_callable=predict,
    )

    prediction_task
