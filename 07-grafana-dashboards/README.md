# Grafana Monitoring Dashboards
**Developer:** [Member 4 Name]

## Dashboard 1: Job Recommendations Monitoring

### Panels (6 total)
1. **Total Predictions** (Stat) - Shows 100,000+ predictions
2. **Predictions Timeline** (Time Series) - Volume over time
3. **Job Distribution** (Pie Chart) - Most recommended jobs
4. **Predictions by Source** (Bar Chart) - Webapp vs scheduled
5. **Model Confidence** (Gauge) - Average: 96.9%
6. **Recent Predictions** (Table) - Last 20 predictions

## Dashboard 2: Data Quality Monitoring

### Panels (4 total)
1. **Files Processed** (Stat) - Total files validated
2. **Valid vs Invalid** (Time Series) - Quality over time
3. **Error Rate** (Gauge) - Average error percentage
4. **Criticality Distribution** (Pie Chart) - High/medium/low

## Setup

1. **Install Grafana**
   - Windows: Download from grafana.com
   - Or use Docker: \docker run -p 3000:3000 grafana/grafana\

2. **Add PostgreSQL Data Source**
   - Host: \localhost:5432\
   - Database: \job_recommendation\
   - User: \job_user\
   - Password: \JobUser@123\

3. **Import Queries**
   - Use queries from \queries.sql\
   - Create panels manually or import JSON

## Access
http://localhost:3000  
Login: admin/admin

## Features
- Real-time updates (refresh every 5-10 seconds)
- Color-coded thresholds
- Interactive visualizations
- Export capabilities
