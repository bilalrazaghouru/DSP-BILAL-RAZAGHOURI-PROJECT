# DSP Project - Member Contributions
 @"
# AI Job Recommendation System - MLOps Project
**Course:** Data Science in Production  
**Repository:** https://github.com/bilalrazaghouru/DSP-BILAL-RAZAGHOURI-PROJE  
**Team Size:** 4 members

## 🎯 Project Overview
Production-ready ML system that recommends jobs to candidates with automated data quality validation and real-time monitoring.

## 📊 Key Statistics
- **Total Predictions:** 100,000+
- **Model Confidence:** 96.9%
- **Data Quality Checks:** 7 validation rules
- **Monitoring Panels:** 10 Grafana visualizations
- **Automation:** 2 Airflow DAGs running every 2-5 minutes

## 👥 Team Members & Contributions

### Member 1: Bilal Razaghouri (Project Lead)
**Folders:** \`01-database-setup/\`, \`02-ml-model/\`, \`03-api-service/\`
- PostgreSQL database design & setup

  ### Member 2: Vamshi Krishna
- ML model training (Random Forest, 97% accuracy)
- 
### Member 3: Fahad Rehman
- FastAPI service with 2 endpoints
- 100,000+ predictions processed

### Member 2: Muhammad Ahmad Shamoon
**Folder:** \`04-streamlit-webapp/\`
- Interactive web interface
- Single & batch prediction forms
- Past predictions viewer with filters
- Real-time results display

### Member 3: Bilal Raza Ghouri
**Folders:** \`05-airflow-dags/\`, \`06-data-validation/\`
- 2 Airflow DAGs (5 + 2 tasks)
- Data quality validation (7 checks)
- HTML report generation
- Automated data splitting

### Member 4:Bilal Raza Ghouri
**Folder:** \`07-grafana-dashboards/\`
- 2 Grafana dashboards
- 10 visualization panels
- Real-time monitoring
- SQL query optimization

## 🏗️ System Architecture

\`\`\`
┌─────────────┐
│  Streamlit  │ ← User Interface
└──────┬──────┘
       │
┌──────▼──────┐
│   FastAPI   │ ← Model Service
└──────┬──────┘
       │
┌──────▼──────┐
│ PostgreSQL  │ ← Database (100K+ records)
└──────┬──────┘
       │
  ┌────┴────┬────────┐
  │         │        │
┌─▼──┐  ┌──▼──┐  ┌──▼──┐
│Air-│  │Data │  │Gra- │
│flow│  │Val  │  │fana │
└────┘  └─────┘  └─────┘
\`\`\`

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL 16
- Docker Desktop

### Installation

\`\`\`bash
# Clone repository
git clone https://github.com/bilalrazaghouru/DSP-BILAL-RAZAGHOURI-PROJE.git
cd DSP-BILAL-RAZAGHOURI-PROJE

# Setup database (Member 1)
cd 01-database-setup
python schema.py

# Train model (Member 1)
cd ../02-ml-model
python train_model.py

# Start API (Terminal 1)
cd ../03-api-service
pip install -r requirements.txt
python main.py

# Start Web App (Terminal 2)
cd ../04-streamlit-webapp
pip install -r requirements.txt
streamlit run app.py

# Start Airflow (Terminal 3)
cd ../05-airflow-dags
docker compose up -d
\`\`\`

### Access Points
| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit | http://localhost:8501 | None |
| FastAPI | http://localhost:8000/docs | None |
| Airflow | http://localhost:8080 | admin/admin |
| Grafana | http://localhost:3000 | admin/admin |

## 📁 Project Structure

\`\`\`
DSP-BILAL-RAZAGHOURI-PROJE/
├── 01-database-setup/          # Bilal - Database schema
├── 02-ml-model/                # Bilal - ML training
├── 03-api-service/             # Bilal - FastAPI
├── 04-streamlit-webapp/        # Member 2 - Web UI
├── 05-airflow-dags/            # Member 3 - Pipelines
├── 06-data-validation/         # Member 3 - Validation
├── 07-grafana-dashboards/      # Member 4 - Monitoring
├── 08-documentation/           # All members
│   ├── SETUP_GUIDE.md
│   ├── DEMO_SCRIPT.md
│   └── TEAM_CONTRIBUTIONS.md
├── data/                       # Sample data only
├── .gitignore
├── README.md
└── requirements.txt
\`\`\`

## 🔧 Technology Stack
- **ML:** scikit-learn, pandas, numpy
- **API:** FastAPI, Uvicorn, SQLAlchemy
- **Web:** Streamlit
- **Database:** PostgreSQL 16
- **Orchestration:** Apache Airflow 2.9
- **Monitoring:** Grafana
- **Containerization:** Docker

## 📊 Project Statistics
- **Total Lines of Code:** ~2,100
- **Development Hours:** 162 hours
- **Git Commits:** 50+
- **Components:** 7 major
- **Documentation:** 8 pages

## 🎓 Features Demonstrated

✅ **ML Model Deployment** - Production-ready model serving  
✅ **API Development** - RESTful API with FastAPI  
✅ **Web Interface** - Interactive UI with Streamlit  
✅ **Database Management** - PostgreSQL with 100K+ records  
✅ **Workflow Orchestration** - Automated pipelines with Airflow  
✅ **Data Quality** - 7 validation rules with reports  
✅ **Real-time Monitoring** - Grafana dashboards  
✅ **Containerization** - Docker for consistency  

## 📝 Documentation
- **Setup Guide:** \`08-documentation/SETUP_GUIDE.md\`
- **Demo Script:** \`08-documentation/DEMO_SCRIPT.md\`
- **Team Contributions:** \`08-documentation/TEAM_CONTRIBUTIONS.md\`

## 📧 Contact
**Project Lead:** Bilal Razaghouri  
**Repository:** https://github.com/bilalrazaghouru/DSP-BILAL-RAZAGHOURI-PROJE

---
**Status:** Production Ready ✅  
**Last Updated:** November 2025
"@ | Out-File -FilePath README.md -Encoding utf8

