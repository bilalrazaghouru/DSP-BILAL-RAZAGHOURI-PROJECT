# 🧠 DSP Project – FastAPI + Streamlit + Airflow + PostgreSQL

A complete **end-to-end data automation project** integrating:
- FastAPI for model predictions  
- Streamlit for interactive UI  
- Airflow for orchestration (data ingestion & prediction pipelines)  
- PostgreSQL (Docker) for storing all data  
- SQLAlchemy ORM for easy database interaction

---

## 📁 Project Structure

DSP-BILAL-RAZAGHOURI-PROJECT/
│
├── api/ # FastAPI backend
│ ├── main.py
│ ├── models.py
│ ├── database.py
│ └── create_tables.py
│
├── webapp/ # Streamlit app
│ └── app.py
│
├── dags/ # Airflow DAGs
│ ├── prediction_dag.py
│ └── ingest_dag.py
│
├── scripts/ # Utility scripts
│ └── generate_data.py
│
├── data/ # Main dataset
│ └── main.csv
│
├── requirements.txt
├── docker-compose.yaml
└── README.md

## ⚙️ Technologies Used

| Tool | Purpose |
|------|----------|
| **Python** | Main programming language |
| **FastAPI** | Backend API for predictions |
| **Streamlit** | Web dashboard for user interface |
| **Airflow** | Automates ingestion & prediction tasks |
| **PostgreSQL (Docker)** | Database for predictions & stats |
| **SQLAlchemy** | ORM for database access |

---

## 🔄 Workflow Overview

1️⃣ **Data Generation** → `scripts/generate_data.py` creates raw CSV files  
2️⃣ **Ingestion DAG** → reads raw data → validates → saves stats → moves good/bad data  
3️⃣ **Prediction DAG** → detects new files → calls FastAPI → stores predictions  
4️⃣ **Streamlit App** → make live predictions and view past results  
5️⃣ **PostgreSQL** stores all predictions and data quality stats

---

## 🗂️ Database Models

### Prediction
Stores every prediction made by API or DAG.

| Column | Description |
|---------|-------------|
| id | Auto ID |
| created_at | Timestamp |
| source | webapp / scheduled |
| features | Input features |
| prediction | Output result |

### DataQualityStat
Stores quality metrics for each ingested file.

| Column | Description |
|---------|-------------|
| file_name | CSV name |
| record_count | Number of rows |
| null_rate | % of missing values |
| criticality | low / medium / high |
| summary | Short summary text |

---

## 🚀 How to Run the Project

### 1️⃣ Start PostgreSQL (Docker)

docker start dsp-pg 2>/dev/null || docker run --name dsp-pg \
-e POSTGRES_USER=dsp -e POSTGRES_PASSWORD=dsp -e POSTGRES_DB=dsp \
-p 5432:5432 -d postgres:16
2️⃣ Create environment & install requirements

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
3️⃣ Create tables

export DATABASE_URL="postgresql+psycopg2://dsp:dsp@localhost:5432/dsp"
python api/create_tables.py
4️⃣ Run FastAPI

python -m uvicorn api.main:app --reload --port 8000
5️⃣ Run Streamlit

streamlit run webapp/app.py
6️⃣ Run Airflow

source ~/airflow-venv/bin/activate
export AIRFLOW_HOME="$HOME/airflow_home"
airflow scheduler
airflow webserver --port 8081

👥 Team Members
Name	Role	Branch
Bilal Razaghouru	Project Lead (API, DB, Integration)	main
Member 1 (Ahmad)	Streamlit UI	streamlit-branch
Member 2 (Fahad)	Airflow DAGs	dag-branch
Member 3 (Vinood)	Airflow Setup / Validation	airflow-branch

✅ Project Highlights
Fully automated ML pipeline

Works end-to-end: ingestion → prediction → visualization

Real-time UI + database integration

Collaborative version control with Git branches

🏁 Results
Streamlit shows live predictions

Airflow automates data movement

PostgreSQL stores data-quality stats

Each team member’s contribution visible on GitHub

Streamlit app running

Airflow DAG view

Database table view

🧾 License
This project is for academic and learning purposes under the guidance of the DSP course.



---

Would you like me to create this README as a **ready-to-download file (`README.md`)** so you can directly upload it to GitHub?
