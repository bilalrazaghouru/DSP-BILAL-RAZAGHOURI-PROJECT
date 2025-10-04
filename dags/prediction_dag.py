from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import os, requests

GOOD = "good-data"

def check_for_new_data():
    files = os.listdir(GOOD)
    return "make_predictions" if files else "skip_task"

def make_predictions():
    files = os.listdir(GOOD)
    for f in files:
        with open(os.path.join(GOOD, f), "rb") as fp:
            response = requests.post("http://127.0.0.1:8000/predict", files={"file": fp})
            print(response.json())

def skip_task():
    print("No new files. Skipping run.")

with DAG(
    dag_id="prediction_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    check_task = BranchPythonOperator(
        task_id="check_for_new_data",
        python_callable=check_for_new_data
    )

    pred_task = PythonOperator(
        task_id="make_predictions",
        python_callable=make_predictions
    )

    skip = PythonOperator(
        task_id="skip_task",
        python_callable=skip_task
    )

    check_task >> [pred_task, skip]

