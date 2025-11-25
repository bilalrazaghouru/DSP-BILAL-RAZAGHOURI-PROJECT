from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

default_args = {'owner': 'airflow', 'start_date': datetime(2024, 1, 1)}
dag = DAG('scheduled_predictions', default_args=default_args, schedule_interval='*/2 * * * *', catchup=False)

def make_predictions(**context):
    good_data_folder = '/opt/airflow/data/good_data'
    
    for file in os.listdir(good_data_folder):
        if file.endswith('.csv'):
            filepath = os.path.join(good_data_folder, file)
            df = pd.read_csv(filepath)
            
            data = df.to_dict('records')
            response = requests.post(
                'http://host.docker.internal:8000/predict',
                json=data,
                params={'source': 'scheduled'}
            )
            print(f"Predicted {file}: {response.status_code}")

PythonOperator(task_id='make_predictions', python_callable=make_predictions, dag=dag)