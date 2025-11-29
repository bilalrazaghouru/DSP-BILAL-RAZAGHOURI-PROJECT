-- Grafana Dashboard SQL Queries
-- Developed by: [Member 4 Name]

-- DASHBOARD 1: Job Recommendations Monitoring

-- Panel 1: Total Predictions (Stat)
SELECT COUNT(*) as value FROM predictions;

-- Panel 2: Predictions Over Time (Time Series)
SELECT 
  timestamp as time,
  predicted_job as metric,
  1 as value
FROM predictions
ORDER BY timestamp;

-- Panel 3: Job Distribution (Pie Chart)
SELECT 
  predicted_job as metric,
  COUNT(*) as value
FROM predictions
GROUP BY predicted_job;

-- Panel 4: Predictions by Source (Bar Chart)
SELECT 
  source,
  COUNT(*) as value
FROM predictions
GROUP BY source;

-- Panel 5: Model Confidence (Gauge)
SELECT 
  AVG(prediction_probability) * 100 as value
FROM predictions;

-- Panel 6: Recent Predictions (Table)
SELECT 
  timestamp,
  predicted_job,
  education_level,
  location,
  ROUND(CAST(prediction_probability * 100 AS numeric), 1) as confidence,
  source
FROM predictions
ORDER BY timestamp DESC
LIMIT 20;

-- ==============================================
-- DASHBOARD 2: Data Quality Monitoring
-- ==============================================

-- Panel 1: Files Processed (Stat)
SELECT COUNT(*) as value FROM data_quality;

-- Panel 2: Valid vs Invalid Data (Time Series)
SELECT 
  timestamp as time,
  valid_rows,
  invalid_rows
FROM data_quality
ORDER BY timestamp;

-- Panel 3: Error Rate (Gauge)
SELECT 
  AVG(invalid_rows::float / NULLIF(total_rows, 0) * 100) as value
FROM data_quality;

-- Panel 4: Criticality Distribution (Pie Chart)
SELECT 
  criticality,
  COUNT(*) as value
FROM data_quality
GROUP BY criticality;
