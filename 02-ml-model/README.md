# Machine Learning Model
**Developer:** Bilal Razaghouri

## Model Details
- **Algorithm:** Random Forest Classifier
- **Features:** experience_years, skill_count, education_encoded, location_encoded
- **Target:** job_title
- **Accuracy:** ~97%

## Training
\\\ash
python train_model.py
\\\

## Model Files
- \job_recommender_model.pkl\ - Trained Random Forest model (87 MB)
- \education_encoder.pkl\ - LabelEncoder for education levels
- \location_encoder.pkl\ - LabelEncoder for locations
- \job_encoder.pkl\ - LabelEncoder for job titles
- \eature_names.pkl\ - List of feature names

## Performance
- Training time: ~2 hours
- Predictions made: 100,000+
- Confidence: 96.9% average
