# 05-airflow-dags/dags/scheduled_predictions.py
# DAG for making predictions on validated data

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from pathlib import Path

# Setup paths
repo_root = Path(__file__).resolve().parents[2]
airflow_root = Path(os.environ.get("AIRFLOW_DIR", repo_root / "05-airflow-dags"))

GOOD_DATA_FOLDER = os.environ.get(
    "GOOD_DATA_FOLDER",
    str(airflow_root / "data" / "good_data")
)
API_URL = os.environ.get("API_URL", "http://host.docker.internal:8001")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "scheduled_predictions",
    default_args=default_args,
    description="Make predictions on validated data",
    schedule_interval="*/2 * * * *",  # Every 2 minutes
    catchup=False,
    tags=["predictions", "ml"],
)


def check_new_data(**context) -> bool:
    """
    Check if there are new CSV files in GOOD_DATA_FOLDER.
    Returns True to continue the DAG, False to skip downstream tasks.
    Also pushes the list of files to XCom.
    """

    if not os.path.exists(GOOD_DATA_FOLDER):
        print(f"⚠️ {GOOD_DATA_FOLDER} doesn't exist")
        os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
        print("📁 Created good_data folder, but no files yet.")
        return False

    files = [f for f in os.listdir(GOOD_DATA_FOLDER) if f.endswith(".csv")]

    if not files:
        print(f"⚠️ No CSV files found in {GOOD_DATA_FOLDER}")
        return False

    print(f"✅ Found {len(files)} file(s) to process:")
    for f in files:
        print(f"   • {f}")

    # Store file list for the next task
    context["task_instance"].xcom_push(key="files_to_process", value=files)
    return True


def make_predictions(**context):
    """Make predictions on files in good_data folder discovered by check_new_data"""

    ti = context["task_instance"]
    files = ti.xcom_pull(key="files_to_process", task_ids="check_new_data")

    # Safety check in case something went wrong
    if not files:
        print("⚠️ No files received from check_new_data, nothing to do.")
        return

    total_predictions = 0
    successful_files = 0
    failed_files = 0
    processed_files = []
    failed_file_names = []

    print(f"📊 Processing {len(files)} files from: {GOOD_DATA_FOLDER}")

    for file in files:
        filepath = os.path.join(GOOD_DATA_FOLDER, file)

        # File might have been removed between tasks
        if not os.path.exists(filepath):
            print(f"   ⚠️ File {file} no longer exists, skipping.")
            failed_files += 1
            failed_file_names.append(file)
            continue

        try:
            # Read CSV
            df = pd.read_csv(filepath)
            print(f"\n📄 Processing {file} ({len(df)} rows)...")

            # Prepare data for API
            data = df.to_dict("records")

            # Call prediction API
            response = requests.post(
                f"{API_URL}/predict",
                json=data,
                params={"source": "scheduled"},
                timeout=60,
            )

            if response.status_code == 200:
                results = response.json()
                n_preds = len(results)
                total_predictions += n_preds
                successful_files += 1
                processed_files.append(file)

                # Delete processed file
                os.remove(filepath)

                print(f"   ✅ {n_preds} predictions made")
                print(f"   🗑️  Deleted {file}")

            else:
                failed_files += 1
                failed_file_names.append(file)
                print(f"   ❌ API Error {response.status_code}: {response.text}")

        except requests.exceptions.ConnectionError:
            failed_files += 1
            failed_file_names.append(file)
            print(f"   ❌ Cannot connect to API at {API_URL}")
            print("   Make sure FastAPI is running!")
            # Stop processing remaining files for this batch
            break

        except Exception as e:
            failed_files += 1
            failed_file_names.append(file)
            print(f"   ❌ Error processing {file}: {str(e)}")

    # Summary
    print(f"\n{'=' * 60}")
    print("BATCH PREDICTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Files processed successfully: {successful_files}")
    print(f"Files failed:              {failed_files}")
    print(f"Total predictions:         {total_predictions}")
    print(f"Processed files:           {processed_files}")
    print(f"Failed files:              {failed_file_names}")
    print(f"{'=' * 60}\n")

    # Push to XCom for notification / tracking
    ti.xcom_push(key="total_predictions", value=total_predictions)
    ti.xcom_push(key="successful_files", value=successful_files)
    ti.xcom_push(key="failed_files", value=failed_files)
    ti.xcom_push(key="processed_files", value=processed_files)
    ti.xcom_push(key="failed_file_names", value=failed_file_names)

    return {
        "total_predictions": total_predictions,
        "successful_files": successful_files,
        "failed_files": failed_files,
        "processed_files": processed_files,
        "failed_file_names": failed_file_names,
    }


def send_summary_notification(**context):
    """Send summary notification (simulated)"""

    ti = context["task_instance"]
    total_predictions = ti.xcom_pull(key="total_predictions", task_ids="make_predictions")
    successful_files = ti.xcom_pull(key="successful_files", task_ids="make_predictions")
    failed_files = ti.xcom_pull(key="failed_files", task_ids="make_predictions")
    processed_files = ti.xcom_pull(key="processed_files", task_ids="make_predictions")
    failed_file_names = ti.xcom_pull(key="failed_file_names", task_ids="make_predictions")

    if total_predictions is None:
        print("No predictions were made in this run.")
        return

    processed_files = processed_files or []
    failed_file_names = failed_file_names or []

    notification = f"""
    🎯 PREDICTION BATCH COMPLETE

    Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    Results:
    • Predictions Made:   {total_predictions}
    • Files Succeeded:    {successful_files}
    • Files Failed:       {failed_files}

    Processed Files:
    {os.linesep.join(['    - ' + f for f in processed_files]) if processed_files else '    (none)'}

    Failed Files:
    {os.linesep.join(['    - ' + f for f in failed_file_names]) if failed_file_names else '    (none)'}

    Status: {'✅ SUCCESS' if failed_files == 0 else '⚠️ PARTIAL SUCCESS' if successful_files > 0 else '❌ FAILED'}
    """

    print(notification)
    print("📧 Notification sent (simulated)")
    # In production, send to Slack/Teams/Email here


# Tasks
check_new_data_task = ShortCircuitOperator(
    task_id="check_new_data",
    python_callable=check_new_data,
    dag=dag,
)

make_predictions_task = PythonOperator(
    task_id="make_predictions",
    python_callable=make_predictions,
    dag=dag,
)

send_notification_task = PythonOperator(
    task_id="send_notification",
    python_callable=send_summary_notification,
    dag=dag,
)

# Dependencies: Prediction DAG (Every 2 minutes)
# check_new_data → make_predictions → send_notification
check_new_data_task >> make_predictions_task >> send_notification_task
