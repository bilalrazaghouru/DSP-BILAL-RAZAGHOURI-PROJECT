# FastAPI Model Service
**Developer:** Bilal Razaghouri

## Endpoints

### POST /predict
Make predictions for one or more candidates.

**Request Body:**
\\\json
[{
  "experience_years": 5.0,
  "skills": "Python,SQL,ML",
  "education_level": "Master",
  "location": "New York"
}]
\\\

**Response:**
\\\json
[{
  "predicted_job": "Data Scientist",
  "probability": 0.95,
  "features_used": {...}
}]
\\\

### GET /past-predictions
Retrieve historical predictions with filters.

**Query Parameters:**
- \start_date\: Filter by start date
- \end_date\: Filter by end date
- \source\: Filter by source (webapp/scheduled/all)

## Running
\\\ash
pip install -r requirements.txt
python main.py
\\\

Access: http://localhost:8000/docs

## Statistics
- Total predictions: 100,000+
- Average response time: <100ms
- Concurrent requests: Supported
