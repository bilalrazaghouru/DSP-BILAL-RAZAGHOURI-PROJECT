# Database Setup
**Developer:** Bilal Razaghouri

## Description
PostgreSQL database schema for storing predictions and data quality metrics.

## Tables
1. **predictions** - Stores all job predictions
   - id, experience_years, skill_count, education_level, location
   - predicted_job, prediction_probability, source, timestamp

2. **data_quality** - Stores data validation results
   - id, filename, total_rows, valid_rows, invalid_rows
   - error_types, criticality, timestamp

## Setup
\\\sql
-- Create database
CREATE DATABASE job_recommendation;
CREATE USER job_user WITH PASSWORD 'JobUser@123';
GRANT ALL PRIVILEGES ON DATABASE job_recommendation TO job_user;

-- Initialize tables
python schema.py
\\\

## Files
- \schema.py\ - Database schema definition with SQLAlchemy
