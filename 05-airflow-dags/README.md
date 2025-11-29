# Airflow Data Pipelines
**Developer:** [Member 3 Name]

## DAGs

### 1. data_ingestion_with_validation
**Schedule:** Every 5 minutes

**Tasks:**
1. \
ead_data\ - Reads random CSV from raw-data folder
2. \alidate_data\ - Validates with 7 quality checks
3. \save_statistics\ - Saves metrics to database
4. \generate_and_send_report\ - Creates HTML report
5. \split_and_save_data\ - Splits into good/bad data

**File:** \dags/data_ingestion_with_validation.py\ (17,147 bytes)

### 2. scheduled_predictions
**Schedule:** Every 2 minutes

**Tasks:**
1. \check_for_new_data\ - Checks good_data folder
2. \make_predictions\ - Calls API to predict

**File:** \dags/scheduled_predictions.py\ (1,032 bytes)

## Running
Ensure you are inside `05-airflow-dags` and run:
```bash
docker compose up -d
```

Note: DAGs are now located in `05-airflow-dags/dags/`. The `docker-compose.yml` mounts `./dags` to `/opt/airflow/dags` in the container.

Access: http://localhost:8080  
Login: admin/admin

## Statistics
- Files processed: Automatic
- Success rate: Monitored in Airflow UI
- Execution time: Visible in logs
