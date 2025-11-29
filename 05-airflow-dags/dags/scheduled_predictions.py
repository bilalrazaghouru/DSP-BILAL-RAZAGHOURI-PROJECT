# 05-airflow-dags/scheduled_predictions.py
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import sys

# Add project paths
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/01-database-setup')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'scheduled_predictions',
    default_args=default_args,
    description='Make predictions on validated data files',
    schedule_interval='*/2 * * * *',  # Every 2 minutes
    catchup=False,
    tags=['predictions', 'ml', 'batch'],
)

# Configuration - Updated path
GOOD_DATA_FOLDER = '/opt/airflow/data/good_data'
API_URL = 'http://host.docker.internal:8000'


def check_new_data(**context):
    """Return True only if there are CSV files to process."""
    if not os.path.exists(GOOD_DATA_FOLDER):
        print(f"⚠️ {GOOD_DATA_FOLDER} does not exist")
        os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
        print("📁 Created folder, but no data yet")
        return False

    files = [f for f in os.listdir(GOOD_DATA_FOLDER) if f.endswith('.csv')]
    if not files:
        print("⚠️ No files to process in good_data")
        return False

    print(f"✅ Found {len(files)} file(s) to process: {files}")
    return True


def make_predictions(**context):
    """Make predictions on all files in good_data folder"""

    if not os.path.exists(GOOD_DATA_FOLDER):
        print(f"⚠️ {GOOD_DATA_FOLDER} does not exist")
        os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
        return

    files = [f for f in os.listdir(GOOD_DATA_FOLDER) if f.endswith('.csv')]

    if not files:
        print("⚠️ No files to process in good_data")
        return

    print(f"📂 Found {len(files)} files to process")
    total_predictions = 0

    for file in files:
        filepath = os.path.join(GOOD_DATA_FOLDER, file)

        try:
            print(f"\n🔮 Processing {file}...")
            df = pd.read_csv(filepath)
            print(f"   📊 Loaded {len(df)} rows")

            # Convert to API format
            data = df.to_dict('records')

            # Make prediction request
            response = requests.post(
                f'{API_URL}/predict',
                json=data,
                params={'source': 'scheduled'},
                timeout=60
            )

            if response.status_code == 200:
                results = response.json()
                print(f"   ✅ Predicted {len(results)} candidates from {file}")
                total_predictions += len(results)

                # Delete file after successful processing
                os.remove(filepath)
                print(f"   🗑️  Deleted {file} from good_data")
            else:
                print(f"   ❌ Error {response.status_code}: {response.text}")

        except requests.exceptions.ConnectionError:
            print(f"   ❌ Cannot connect to API at {API_URL}")
            print("   ⚠️  Make sure FastAPI is running")
            break
        except Exception as e:
            print(f"   ❌ Error processing {file}: {str(e)}")

    print(f"\n✅ Batch complete: {total_predictions} total predictions made")

    # Push metrics to XCom
    context['task_instance'].xcom_push(key='files_processed', value=len(files))
    context['task_instance'].xcom_push(key='total_predictions', value=total_predictions)

    return total_predictions


# Tasks
check_new_data_task = ShortCircuitOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    dag=dag,
)

make_predictions_task = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    dag=dag,
)

# Dependency
check_new_data_task >> make_predictions_task
