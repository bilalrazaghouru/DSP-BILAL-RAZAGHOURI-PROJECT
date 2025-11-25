from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import os
import random
import json
import sys

# Add project path
sys.path.append('/opt/airflow')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_ingestion_with_validation',
    default_args=default_args,
    description='Ingest and validate job candidate data with Great Expectations',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['data-quality', 'validation', 'ingestion'],
)

# Configuration
RAW_DATA_FOLDER = '/opt/airflow/data/raw-data'
GOOD_DATA_FOLDER = '/opt/airflow/data/good_data'
BAD_DATA_FOLDER = '/opt/airflow/data/bad_data'
REPORTS_FOLDER = '/opt/airflow/reports'


def read_random_file(**context):
    """Task 1: Read one random file from raw-data folder"""
    
    files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith('.csv')]
    
    if not files:
        print("⚠️ No files to process")
        context['task_instance'].xcom_push(key='skip_dag', value=True)
        return None
    
    # Select random file
    selected_file = random.choice(files)
    filepath = os.path.join(RAW_DATA_FOLDER, selected_file)
    
    print(f"📂 Selected file: {selected_file}")
    
    # Read data
    df = pd.read_csv(filepath)
    print(f"📊 Loaded {len(df)} rows")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='filename', value=selected_file)
    context['task_instance'].xcom_push(key='filepath', value=filepath)
    context['task_instance'].xcom_push(key='row_count', value=len(df))
    context['task_instance'].xcom_push(key='data_json', value=df.to_json())
    context['task_instance'].xcom_push(key='skip_dag', value=False)
    
    # Delete the file after reading
    os.remove(filepath)
    print(f"🗑️ Deleted {selected_file} from raw-data")
    
    return selected_file


def validate_data_quality(**context):
    """Task 2: Validate data quality using custom validator"""
    
    skip = context['task_instance'].xcom_pull(key='skip_dag', task_ids='read_data')
    if skip:
        print("Skipping validation - no data to process")
        return
    
    filename = context['task_instance'].xcom_pull(key='filename', task_ids='read_data')
    data_json = context['task_instance'].xcom_pull(key='data_json', task_ids='read_data')
    
    # Reconstruct DataFrame
    df = pd.read_json(data_json)
    
    print(f"🔍 Validating {filename}...")
    
    # Validation rules
    validation_rules = {
        'required_columns': ['experience_years', 'skills', 'education_level', 'location'],
        'valid_education': ['High School', 'Bachelor', 'Master', 'PhD', 'Entry Level', 'Mid Level', 'Senior Level', 'Expert'],
        'experience_min': 0,
        'experience_max': 70
    }
    
    errors = []
    valid_rows_mask = pd.Series([True] * len(df))
    
    # Rule 1: Check required columns
    missing_cols = [col for col in validation_rules['required_columns'] if col not in df.columns]
    if missing_cols:
        errors.append({
            'type': 'missing_columns',
            'severity': 'high',
            'message': f"Missing columns: {missing_cols}",
            'count': len(missing_cols)
        })
        valid_rows_mask[:] = False  # All rows invalid if columns missing
    
    if not missing_cols:  # Only check further if columns exist
        
        # Rule 2: Missing values
        for col in validation_rules['required_columns']:
            missing = df[col].isna()
            if missing.sum() > 0:
                errors.append({
                    'type': 'missing_values',
                    'severity': 'high',
                    'message': f"Missing values in {col}",
                    'count': int(missing.sum())
                })
                valid_rows_mask &= ~missing
        
        # Rule 3: Type errors (non-numeric experience)
        if 'experience_years' in df.columns:
            numeric_exp = pd.to_numeric(df['experience_years'], errors='coerce')
            non_numeric = numeric_exp.isna() & df['experience_years'].notna()
            if non_numeric.sum() > 0:
                errors.append({
                    'type': 'type_error',
                    'severity': 'high',
                    'message': "Non-numeric experience values",
                    'count': int(non_numeric.sum())
                })
                valid_rows_mask &= ~non_numeric
        
        # Rule 4: Out of range values
        if 'experience_years' in df.columns:
            numeric_exp = pd.to_numeric(df['experience_years'], errors='coerce')
            out_range = (numeric_exp < 0) | (numeric_exp > 70)
            if out_range.sum() > 0:
                errors.append({
                    'type': 'out_of_range',
                    'severity': 'medium',
                    'message': "Experience outside 0-70 range",
                    'count': int(out_range.sum())
                })
                valid_rows_mask &= ~out_range
        
        # Rule 5: Invalid education levels
        if 'education_level' in df.columns:
            invalid_edu = ~df['education_level'].isin(validation_rules['valid_education'])
            if invalid_edu.sum() > 0:
                errors.append({
                    'type': 'invalid_category',
                    'severity': 'medium',
                    'message': "Invalid education levels",
                    'count': int(invalid_edu.sum())
                })
                valid_rows_mask &= ~invalid_edu
        
        # Rule 6: Empty skills
        if 'skills' in df.columns:
            empty_skills = df['skills'].str.strip().str.len() == 0
            if empty_skills.sum() > 0:
                errors.append({
                    'type': 'empty_value',
                    'severity': 'medium',
                    'message': "Empty skills field",
                    'count': int(empty_skills.sum())
                })
                valid_rows_mask &= ~empty_skills
        
        # Rule 7: Duplicates
        duplicates = df.duplicated()
        if duplicates.sum() > 0:
            errors.append({
                'type': 'duplicates',
                'severity': 'low',
                'message': "Duplicate rows",
                'count': int(duplicates.sum())
            })
            valid_rows_mask &= ~duplicates
    
    # Calculate statistics
    valid_rows = int(valid_rows_mask.sum())
    invalid_rows = len(df) - valid_rows
    total_errors = sum(error['count'] for error in errors)
    
    # Determine criticality
    error_rate = invalid_rows / len(df) if len(df) > 0 else 0
    if error_rate >= 0.5 or missing_cols:
        criticality = 'high'
    elif error_rate >= 0.2:
        criticality = 'medium'
    else:
        criticality = 'low'
    
    validation_results = {
        'success': len(errors) == 0,
        'filename': filename,
        'total_rows': len(df),
        'valid_rows': valid_rows,
        'invalid_rows': invalid_rows,
        'total_errors': total_errors,
        'criticality': criticality,
        'errors': errors
    }
    
    # Push to XCom
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    context['task_instance'].xcom_push(key='valid_rows_mask', value=valid_rows_mask.tolist())
    
    print(f"✅ Validation complete:")
    print(f"   Valid: {valid_rows}, Invalid: {invalid_rows}")
    print(f"   Criticality: {criticality}")
    print(f"   Error types: {len(errors)}")
    
    return validation_results


def save_statistics(**context):
    """Task 3: Save validation statistics to database"""
    
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='validate_data')
    
    if not validation_results:
        print("No validation results to save")
        return
    
    # Database connection (using host.docker.internal for Docker)
    DATABASE_URL = "postgresql://job_user:JobUser@123@host.docker.internal:5432/job_recommendation"
    engine = create_engine(DATABASE_URL)
    
    # Insert statistics
    insert_query = f"""
    INSERT INTO data_quality (filename, total_rows, valid_rows, invalid_rows, error_types, criticality, timestamp)
    VALUES (
        '{validation_results['filename']}',
        {validation_results['total_rows']},
        {validation_results['valid_rows']},
        {validation_results['invalid_rows']},
        '{json.dumps(validation_results['errors'])}',
        '{validation_results['criticality']}',
        NOW()
    )
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(insert_query)
            conn.commit()
        print(f"💾 Statistics saved to database")
    except Exception as e:
        print(f"❌ Error saving to database: {e}")
        raise


def generate_and_send_report(**context):
    """Task 4: Generate HTML report and send alert"""
    
    validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='validate_data')
    
    if not validation_results:
        print("No validation results for report")
        return
    
    # Generate HTML report
    filename = validation_results['filename']
    report_filename = f"validation_{filename.replace('.csv', '')}.html"
    report_path = os.path.join(REPORTS_FOLDER, report_filename)
    
    color = 'green' if validation_results['success'] else 'red'
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Validation Report</title>
        <style>
            body {{ font-family: Arial; padding: 20px; background: #f5f5f5; }}
            .container {{ background: white; padding: 30px; border-radius: 8px; max-width: 1200px; margin: 0 auto; }}
            .header {{ border-bottom: 3px solid {color}; padding-bottom: 20px; margin-bottom: 30px; }}
            .success {{ color: green; }}
            .failure {{ color: red; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin: 30px 0; }}
            .stat-box {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }}
            .stat {{ font-size: 36px; font-weight: bold; }}
            .stat-label {{ font-size: 14px; color: #666; margin-top: 10px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #4CAF50; color: white; }}
            .severity-high {{ background-color: #ffebee; }}
            .severity-medium {{ background-color: #fff3e0; }}
            .severity-low {{ background-color: #e8f5e9; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Data Quality Validation Report</h1>
                <h2 class="{'success' if validation_results['success'] else 'failure'}">
                    {'✅ ALL CHECKS PASSED' if validation_results['success'] else '❌ VALIDATION FAILED'}
                </h2>
                <p><strong>File:</strong> {filename}</p>
                <p><strong>Criticality:</strong> <span style="color: {color};">{validation_results['criticality'].upper()}</span></p>
            </div>
            
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="stat">{validation_results['total_rows']}</div>
                    <div class="stat-label">Total Rows</div>
                </div>
                <div class="stat-box">
                    <div class="stat" style="color: green;">{validation_results['valid_rows']}</div>
                    <div class="stat-label">Valid Rows</div>
                </div>
                <div class="stat-box">
                    <div class="stat" style="color: red;">{validation_results['invalid_rows']}</div>
                    <div class="stat-label">Invalid Rows</div>
                </div>
                <div class="stat-box">
                    <div class="stat">{100 - (validation_results['invalid_rows']/validation_results['total_rows']*100 if validation_results['total_rows'] > 0 else 0):.1f}%</div>
                    <div class="stat-label">Quality Score</div>
                </div>
            </div>
    """
    
    if validation_results['errors']:
        html += """
            <h3>🔍 Detected Issues</h3>
            <table>
                <tr>
                    <th>Severity</th>
                    <th>Error Type</th>
                    <th>Description</th>
                    <th>Count</th>
                </tr>
        """
        
        for error in validation_results['errors']:
            html += f"""
                <tr class="severity-{error['severity']}">
                    <td><strong>{error['severity'].upper()}</strong></td>
                    <td>{error['type']}</td>
                    <td>{error['message']}</td>
                    <td><strong>{error['count']}</strong></td>
                </tr>
            """
        
        html += "</table>"
    else:
        html += "<p style='color: green; font-size: 18px; margin-top: 30px;'>✅ No issues detected! Data quality is excellent.</p>"
    
    html += """
        </div>
    </body>
    </html>
    """
    
    # Save report
    os.makedirs(REPORTS_FOLDER, exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"📄 HTML report generated: {report_path}")
    
    # Log alert message (in production, would send to Teams/Slack)
    alert_message = f"""
    ⚠️ DATA QUALITY ALERT
    
    File: {filename}
    Criticality: {validation_results['criticality'].upper()}
    
    Summary:
    - Total Rows: {validation_results['total_rows']}
    - Valid: {validation_results['valid_rows']}
    - Invalid: {validation_results['invalid_rows']}
    - Error Types: {len(validation_results['errors'])}
    
    Report: {report_path}
    """
    
    print(alert_message)
    print("📧 Alert notification sent (simulated)")


def split_and_save_data(**context):
    """Task 5: Split data into good_data and bad_data folders"""
    
    validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='validate_data')
    data_json = context['task_instance'].xcom_pull(key='data_json', task_ids='read_data')
    valid_mask = context['task_instance'].xcom_pull(key='valid_rows_mask', task_ids='validate_data')
    
    if not validation_results or not data_json:
        print("No data to split")
        return
    
    # Reconstruct DataFrame
    df = pd.read_json(data_json)
    valid_mask_series = pd.Series(valid_mask)
    filename = validation_results['filename']
    
    valid_rows = validation_results['valid_rows']
    invalid_rows = validation_results['invalid_rows']
    
    # Create folders
    os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)
    os.makedirs(BAD_DATA_FOLDER, exist_ok=True)
    
    if invalid_rows == 0:
        # All good - save to good_data
        good_path = os.path.join(GOOD_DATA_FOLDER, filename)
        df.to_csv(good_path, index=False)
        print(f"✅ All {valid_rows} rows valid → saved to good_data/{filename}")
        
    elif valid_rows == 0:
        # All bad - save to bad_data
        bad_path = os.path.join(BAD_DATA_FOLDER, filename)
        df.to_csv(bad_path, index=False)
        print(f"❌ All {invalid_rows} rows invalid → saved to bad_data/{filename}")
        
    else:
        # Mixed - split into two files
        good_df = df[valid_mask_series]
        bad_df = df[~valid_mask_series]
        
        good_path = os.path.join(GOOD_DATA_FOLDER, filename)
        bad_path = os.path.join(BAD_DATA_FOLDER, filename)
        
        good_df.to_csv(good_path, index=False)
        bad_df.to_csv(bad_path, index=False)
        
        print(f"📊 Split complete:")
        print(f"   ✅ {len(good_df)} rows → good_data/{filename}")
        print(f"   ❌ {len(bad_df)} rows → bad_data/{filename}")


# Define tasks
read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_random_file,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_quality,
    dag=dag,
)

save_statistics_task = PythonOperator(
    task_id='save_statistics',
    python_callable=save_statistics,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_and_send_report',
    python_callable=generate_and_send_report,
    dag=dag,
)

split_save_task = PythonOperator(
    task_id='split_and_save_data',
    python_callable=split_and_save_data,
    dag=dag,
)

# Set dependencies
read_data_task >> validate_data_task >> [save_statistics_task, generate_report_task, split_save_task]