# 05-airflow-dags/dags/data_ingestion_with_validation.py
# Complete Data Ingestion DAG with Great Expectations and Teams Incoming Webhook Alerts

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import random
import json
from pathlib import Path
import sys
import requests  # for Teams webhook

# ======================================================================
# PATHS (DOCKER-FRIENDLY: /opt/airflow IS PROJECT ROOT)
# ======================================================================

# Inside the container, docker-compose mounts everything under /opt/airflow
project_root = Path("/opt/airflow")

# Add path for data validation code (06-data-validation is mounted there)
sys.path.append(str(project_root / "06-data-validation"))

# Import Great Expectations validator if available
try:
    from validate_data_gx import GreatExpectationsValidator
    USE_GX = True
except Exception:
    USE_GX = False
    print("⚠️ Great Expectations not available, using simple validator")

# Data folders (host: ./data/*; container: /opt/airflow/data/*)
RAW_DATA_FOLDER = os.environ.get(
    "RAW_DATA_FOLDER", str(project_root / "data" / "raw-data")
)
GOOD_DATA_FOLDER = os.environ.get(
    "GOOD_DATA_FOLDER", str(project_root / "data" / "good_data")
)
BAD_DATA_FOLDER = os.environ.get(
    "BAD_DATA_FOLDER", str(project_root / "data" / "bad_data")
)

# Reports folder (host: ./reports; container: /opt/airflow/reports)
REPORTS_FOLDER = os.environ.get(
    "REPORTS_FOLDER", str(project_root / "reports")
)

# Direct Microsoft Teams Incoming Webhook (no Power Automate)
TEAMS_WEBHOOK_URL = os.environ.get("https://epitafr.webhook.office.com/webhookb2/f5d0f70a-a407-4c29-9f5b-6cfa770ba434@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/4655948ff57742b9aeab082c811da0ea/b352da5f-9c6b-4120-8bfc-be25f0856de9/V2zO5E2vyiwWyqKkxrULWuyPcXQC5Fh_eUJ5EeIJTQUaI1", None)

# Default DB URL (can be overridden by env DATABASE_URL)
default_db = "postgresql://job_user:JobUser%40123@host.docker.internal:5432/job_recommendation"


# ======================================================================
# DEFAULT ARGS, FAILURE CALLBACK & DAG
# ======================================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def notify_teams_failure(context):
    """
    Generic failure notification for any task in this DAG using Teams Incoming Webhook.
    Triggered when a task finally fails after all retries.
    """
    if not TEAMS_WEBHOOK_URL:
        print("⚠️ TEAMS_WEBHOOK_URL not set – cannot send failure alert")
        return

    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    exec_date = context["execution_date"]
    log_url = ti.log_url

    text = f"""
❌ *Airflow Task Failed*

**DAG:** {dag_id}  
**Task:** {task_id}  
**Execution time:** {exec_date}

[View logs]({log_url})
"""

    try:
        resp = requests.post(
            TEAMS_WEBHOOK_URL,
            json={"text": text},
            timeout=10,
        )
        if resp.status_code in (200, 204):
            print("✅ Teams failure alert sent via Incoming Webhook")
        else:
            print(f"⚠️ Teams failure alert HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"⚠️ Error sending Teams failure alert: {e}")


dag = DAG(
    "data_ingestion_with_validation",
    default_args=default_args,
    description="Ingest and validate job candidate data with GX and Teams alerts",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    tags=["data-quality", "great-expectations", "ingestion"],
    on_failure_callback=notify_teams_failure,
)


# ======================================================================
# TASK 1: READ-DATA
# ======================================================================

def read_data(**context):
    """
    Read one random file from raw-data folder and delete it.
    Returns: filepath of the selected file.
    """

    print("=" * 60)
    print("TASK 1: READ-DATA")
    print("=" * 60)

    data_folder = RAW_DATA_FOLDER
    print(f"📁 Using RAW_DATA_FOLDER = {data_folder}")

    # Create folder if missing
    if not os.path.exists(data_folder):
        print("⚠️ raw-data folder does not exist, creating it...")
        os.makedirs(data_folder, exist_ok=True)

    files = [f for f in os.listdir(data_folder) if f.endswith(".csv")]

    if not files:
        print("⚠️ No CSV files to process in raw-data")
        context["task_instance"].xcom_push(key="skip_dag", value=True)
        return None

    # Select random file
    selected_file = random.choice(files)
    filepath = os.path.join(data_folder, selected_file)

    print(f"\n📄 Selected file: {selected_file}")
    print(f"📁 Full path: {filepath}")

    try:
        df = pd.read_csv(filepath)
        print(f"📊 Rows: {len(df)}")
        print(f"📋 Columns: {list(df.columns)}")

        ti = context["task_instance"]
        ti.xcom_push(key="filename", value=selected_file)
        ti.xcom_push(key="filepath", value=filepath)
        ti.xcom_push(key="row_count", value=len(df))
        ti.xcom_push(key="data_json", value=df.to_json())
        ti.xcom_push(key="skip_dag", value=False)

        # Delete the file from raw-data
        os.remove(filepath)
        print(f"🗑️  Deleted file from raw-data: {selected_file}")

        print("\n✅ Task 1 Complete\n")
        return filepath

    except Exception as e:
        print(f"❌ Error reading file: {e}")
        import traceback
        traceback.print_exc()
        context["task_instance"].xcom_push(key="skip_dag", value=True)
        return None


# ======================================================================
# TASK 2: VALIDATE-DATA
# ======================================================================

def validate_data(**context):
    """
    Validate data using Great Expectations or simple validation.
    Determines criticality: HIGH, MEDIUM, LOW.
    """

    print("=" * 60)
    print("TASK 2: VALIDATE-DATA")
    print("=" * 60)

    skip = context["task_instance"].xcom_pull(key="skip_dag", task_ids="read_data")
    if skip:
        print("⚠️ Skipping validation - no data to process")
        return

    filename = context["task_instance"].xcom_pull(key="filename", task_ids="read_data")
    data_json = context["task_instance"].xcom_pull(key="data_json", task_ids="read_data")
    df = pd.read_json(data_json)

    print(f"🔍 Validating: {filename}")
    print(f"📊 Rows: {len(df)}")

    try:
        ti = context["task_instance"]

        if USE_GX:
            validator = GreatExpectationsValidator()
            validation_results = validator.validate_job_data(df, filename)

            stats = validation_results.get("statistics", {})
            success_rate = stats.get("success_percent", 100)

            if success_rate < 50:
                criticality = "HIGH"
            elif success_rate < 80:
                criticality = "MEDIUM"
            else:
                criticality = "LOW"

            validation_results["criticality"] = criticality
            validation_results.setdefault("total_rows", len(df))

            # Placeholder mask – all rows considered valid
            valid_rows_mask = [True] * len(df)

            ti.xcom_push(key="validation_results", value=validation_results)
            ti.xcom_push(key="valid_rows_mask", value=valid_rows_mask)

            print("\n✅ Great Expectations validation complete")
            print(f"   Success Rate: {success_rate:.1f}%")
            print(f"   Criticality: {criticality}")

        else:
            print("Using simple validation (GX not available)")
            validation_results = simple_validate(df, filename)
            ti.xcom_push(key="validation_results", value=validation_results)
            ti.xcom_push(key="valid_rows_mask", value=[True] * len(df))

        print("\n✅ Task 2 Complete\n")
        return validation_results

    except Exception as e:
        print(f"❌ Validation error: {e}")
        import traceback
        traceback.print_exc()
        raise


def simple_validate(df, filename):
    """Simple fallback validator"""
    errors = []
    required_cols = ["experience_years", "skills", "education_level", "location"]

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        errors.append(
            {
                "type": "missing_columns",
                "count": len(missing_cols),
                "message": f"Missing: {missing_cols}",
            }
        )

    return {
        "success": len(errors) == 0,
        "filename": filename,
        "total_rows": len(df),
        "valid_rows": len(df) if len(errors) == 0 else 0,
        "invalid_rows": 0 if len(errors) == 0 else len(df),
        "criticality": "HIGH" if errors else "LOW",
        "errors": errors,
        "statistics": {
            "evaluated_expectations": 1,
            "successful_expectations": 1 if len(errors) == 0 else 0,
            "unsuccessful_expectations": len(errors),
            "success_percent": 100 if len(errors) == 0 else 0,
        },
    }


# ======================================================================
# TASK 3: SAVE-STATISTICS (Parallel)
# ======================================================================

def save_statistics(**context):
    """
    Save validation statistics to database.
    """

    print("=" * 60)
    print("TASK 3: SAVE-STATISTICS")
    print("=" * 60)

    from sqlalchemy import create_engine, text

    validation_results = context["task_instance"].xcom_pull(
        key="validation_results", task_ids="validate_data"
    )

    if not validation_results:
        print("⚠️ No validation results to save")
        return

    DATABASE_URL = os.environ.get("DATABASE_URL", default_db)
    engine = create_engine(DATABASE_URL)

    filename = validation_results["filename"]
    total_rows = validation_results.get("total_rows", 0)

    stats = validation_results.get("statistics", {})
    success_rate = stats.get("success_percent", 0)

    valid_rows = int(total_rows * success_rate / 100)
    invalid_rows = total_rows - valid_rows

    criticality = validation_results.get("criticality", "UNKNOWN")
    error_types = json.dumps(
        validation_results.get("results", validation_results.get("errors", []))
    )

    insert_query = text(
        """
    INSERT INTO data_quality 
    (filename, total_rows, valid_rows, invalid_rows, error_types, criticality, timestamp)
    VALUES 
    (:filename, :total_rows, :valid_rows, :invalid_rows, :error_types, :criticality, NOW())
    """
    )

    try:
        with engine.connect() as conn:
            conn.execute(
                insert_query,
                {
                    "filename": filename,
                    "total_rows": total_rows,
                    "valid_rows": valid_rows,
                    "invalid_rows": invalid_rows,
                    "error_types": error_types,
                    "criticality": criticality.lower(),
                },
            )
            conn.commit()

        print("💾 Statistics saved to database:")
        print(f"   Filename: {filename}")
        print(f"   Total rows: {total_rows}")
        print(f"   Valid rows: {valid_rows}")
        print(f"   Invalid rows: {invalid_rows}")
        print(f"   Criticality: {criticality}")

        print("\n✅ Task 3 Complete\n")

    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()


# ======================================================================
# TASK 4: SEND-ALERTS (Parallel) – Direct to Teams Webhook
# ======================================================================

def send_alerts(**context):
    """
    Generate HTML report and send a simple Teams message via Incoming Webhook.
    """

    print("=" * 60)
    print("TASK 4: SEND-ALERTS (Direct Teams Webhook)")
    print("=" * 60)

    validation_results = context["task_instance"].xcom_pull(
        key="validation_results", task_ids="validate_data"
    )

    if not validation_results:
        print("⚠️ No validation results, nothing to alert")
        return

    filename = validation_results["filename"]
    criticality = validation_results.get("criticality", "UNKNOWN")

    # Generate HTML report path
    report_filename = f"validation_{filename.replace('.csv', '')}.html"
    report_path = os.path.join(REPORTS_FOLDER, report_filename)
    os.makedirs(REPORTS_FOLDER, exist_ok=True)

    try:
        if USE_GX:
            from validate_data_gx import GreatExpectationsValidator
            validator = GreatExpectationsValidator()
            validator.generate_html_report(validation_results, report_path)
        else:
            generate_simple_html_report(validation_results, report_path)

        print(f"📄 Report generated: {report_path}")
    except Exception as e:
        print(f"⚠️ Report generation error: {e}")
        generate_simple_html_report(validation_results, report_path)

    stats = validation_results.get("statistics", {})
    success_rate = stats.get("success_percent", 0)
    total_checks = stats.get("evaluated_expectations", 0)
    failed_checks = stats.get("unsuccessful_expectations", 0)

    # Build a short error summary
    results = validation_results.get("results", validation_results.get("errors", []))
    error_lines = []
    for r in results[:5]:
        if not r.get("success", True):
            exp_type = r.get("expectation_type", r.get("type", "Unknown"))
            msg = r.get("message", "Validation failed")
            error_lines.append(f"- {exp_type}: {msg}")

    error_summary = "\n".join(error_lines) if error_lines else "No detailed errors."

    text = f"""
🚨 *Data Quality Alert*

**File:** {filename}  
**Criticality:** {criticality}  
**Success rate:** {success_rate:.1f}%  
**Total checks:** {total_checks}  
**Failed checks:** {failed_checks}  
**Total rows:** {validation_results.get('total_rows', 0)}  

**Errors:**  
{error_summary}

_Local report path (inside Airflow container):_  
`{os.path.abspath(report_path)}`
"""

    if TEAMS_WEBHOOK_URL:
        try:
            response = requests.post(
                TEAMS_WEBHOOK_URL,
                json={"text": text},
                timeout=10,
            )
            if response.status_code in (200, 204):
                print("✅ Teams notification sent via Incoming Webhook")
            else:
                print(
                    f"⚠️ Teams webhook returned {response.status_code}: {response.text}"
                )
        except Exception as e:
            print(f"⚠️ Error sending message to Teams: {e}")
    else:
        print("⚠️ TEAMS_WEBHOOK_URL not set. Message would have been:")
        print(text)

    print("\n✅ Task 4 Complete\n")


def generate_simple_html_report(validation_results, output_path):
    """Generate simple HTML report"""
    success = validation_results.get("success", False)
    color = "#4CAF50" if success else "#f44336"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Validation Report</title>
    <style>
        body {{ font-family: Arial; padding: 20px; background: #f5f5f5; }}
        .container {{ background: white; padding: 30px; border-radius: 8px; max-width: 1000px; margin: 0 auto; }}
        .header {{ border-bottom: 3px solid {color}; padding-bottom: 20px; }}
        h1 {{ color: {color}; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{'✅ VALIDATION PASSED' if success else '❌ VALIDATION FAILED'}</h1>
            <p><strong>File:</strong> {validation_results['filename']}</p>
            <p><strong>Criticality:</strong> {validation_results.get('criticality', 'UNKNOWN')}</p>
        </div>
    </div>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


# ======================================================================
# TASK 5: SPLIT-AND-SAVE-DATA (Parallel)
# ======================================================================

def split_and_save_data(**context):
    """
    Split file based on validation results:
    - All valid → good_data/
    - All invalid → bad_data/
    - Mixed → split good/bad.
    """

    print("=" * 60)
    print("TASK 5: SPLIT-AND-SAVE-DATA")
    print("=" * 60)

    validation_results = context["task_instance"].xcom_pull(
        key="validation_results", task_ids="validate_data"
    )
    data_json = context["task_instance"].xcom_pull(
        key="data_json", task_ids="read_data"
    )
    valid_mask = context["task_instance"].xcom_pull(
        key="valid_rows_mask", task_ids="validate_data"
    )

    if not validation_results or not data_json:
        print("⚠️ No data to split")
        return

    df = pd.read_json(data_json)
    filename = validation_results["filename"]

    total_rows = len(df)

    # Try to use explicit valid/invalid counts if present
    valid_rows = validation_results.get("valid_rows")
    invalid_rows = validation_results.get("invalid_rows")

    if valid_rows is None or invalid_rows is None:
        # Fallback: derive from mask
        if valid_mask is not None:
            valid_mask_series = pd.Series(valid_mask)
            valid_rows = int(valid_mask_series.sum())
            invalid_rows = total_rows - valid_rows
        else:
            valid_rows = total_rows
            invalid_rows = 0

    os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
    os.makedirs(BAD_DATA_FOLDER, exist_ok=True)

    print("📊 Data split analysis:")
    print(f"   Total rows: {total_rows}")
    print(f"   Valid rows: {valid_rows}")
    print(f"   Invalid rows: {invalid_rows}")

    if invalid_rows == 0:
        good_path = os.path.join(GOOD_DATA_FOLDER, filename)
        df.to_csv(good_path, index=False)
        print(f"\n✅ All {total_rows} rows are valid")
        print(f"📁 Saved to: good_data/{filename}")

    elif valid_rows == 0:
        bad_path = os.path.join(BAD_DATA_FOLDER, filename)
        df.to_csv(bad_path, index=False)
        print(f"\n❌ All {total_rows} rows are invalid")
        print(f"📁 Saved to: bad_data/{filename}")

    else:
        valid_mask_series = pd.Series(valid_mask)
        good_df = df[valid_mask_series]
        bad_df = df[~valid_mask_series]

        good_path = os.path.join(GOOD_DATA_FOLDER, filename)
        bad_path = os.path.join(BAD_DATA_FOLDER, filename)

        good_df.to_csv(good_path, index=False)
        bad_df.to_csv(bad_path, index=False)

        print("\n📊 File split into 2:")
        print(f"   ✅ {len(good_df)} valid rows → good_data/{filename}")
        print(f"   ❌ {len(bad_df)} invalid rows → bad_data/{filename}")

    print("\n✅ Task 5 Complete\n")


# ======================================================================
# DEFINE TASKS & DEPENDENCIES
# ======================================================================

read_data_task = PythonOperator(
    task_id="read_data",
    python_callable=read_data,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag,
)

save_statistics_task = PythonOperator(
    task_id="save_statistics",
    python_callable=save_statistics,
    dag=dag,
)

send_alerts_task = PythonOperator(
    task_id="send_alerts",
    python_callable=send_alerts,
    dag=dag,
)

split_and_save_task = PythonOperator(
    task_id="split_and_save_data",
    python_callable=split_and_save_data,
    dag=dag,
)

# read_data → validate_data → [save_statistics, send_alerts, split_and_save_data]
read_data_task >> validate_data_task >> [
    save_statistics_task,
    send_alerts_task,
    split_and_save_task,
]
