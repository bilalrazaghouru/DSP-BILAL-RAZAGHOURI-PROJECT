import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
import joblib
import os
import numpy as np
from pathlib import Path

# Compute repo aware paths
project_root = Path(__file__).resolve().parents[2]
models_dir = project_root / '02-ml-model' / 'models'
data_path = project_root / 'data' / 'job_recommendations.csv'

# Ensure models folder exists
models_dir.mkdir(parents=True, exist_ok=True)

# Load dataset
df = pd.read_csv(data_path)

# Step 1: Rename columns to match expected names
df.rename(columns={
    'Job Title': 'job_title',
    'Location': 'location',
    'Experience Level': 'education_level',   # we’ll treat experience level as education_level
    'Required Skills': 'skills'
}, inplace=True)

# Step 2: Create synthetic "experience_years"
# Map experience levels to numeric values
experience_map = {
    'Entry Level': 1,
    'Mid Level': 3,
    'Senior Level': 7,
    'Director': 10,
    'Executive': 15
}

df['experience_years'] = df['education_level'].map(experience_map)
df['experience_years'].fillna(3, inplace=True)  # default 3 years

#  Step 3: Feature engineering 
df['skill_count'] = df['skills'].astype(str).apply(lambda x: len(x.split(',')))

# Step 4: Encode categorical features
le_education = LabelEncoder()
le_location = LabelEncoder()
le_job = LabelEncoder()

df['education_encoded'] = le_education.fit_transform(df['education_level'].astype(str))
df['location_encoded'] = le_location.fit_transform(df['location'].astype(str))
df['job_encoded'] = le_job.fit_transform(df['job_title'].astype(str))

# Step 5: Prepare features and target
X = df[['experience_years', 'skill_count', 'education_encoded', 'location_encoded']]
y = df['job_encoded']

# Step 6: Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 7: Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Step 8: Save all models
joblib.dump(model, models_dir / 'job_recommender_model.pkl')
joblib.dump(le_education, models_dir / 'education_encoder.pkl')
joblib.dump(le_location, models_dir / 'location_encoder.pkl')
joblib.dump(le_job, models_dir / 'job_encoder.pkl')
joblib.dump(list(X.columns), models_dir / 'feature_names.pkl')

# Step 9: Print result 
accuracy = model.score(X_test, y_test) * 100
print(f"✅ Model trained successfully! Accuracy: {accuracy:.2f}%")
print("✅ Model and encoders saved in the 'models' folder.")
