# 06-data-validation/create_test_data_with_errors.py
# Generate test CSV files with intentional data quality issues

import pandas as pd
import numpy as np
import random
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent.parent.resolve()
raw_data_folder = project_root / '05-airflow-dags' / 'data' / 'raw-data'
raw_data_folder.mkdir(parents=True, exist_ok=True)

print("="*60)
print("GENERATING TEST DATA WITH INTENTIONAL ERRORS")
print("="*60)
print(f"\n📁 Output folder: {raw_data_folder}\n")

# Valid values
valid_education = ['High School', 'Bachelor', 'Master', 'PhD']
valid_locations = ['New York', 'San Francisco', 'London', 'Remote', 'Toronto']
valid_skills = ['Python,SQL', 'Java,Spring', 'JavaScript,React', 'Data,ML', 'Cloud,AWS', 'DevOps,Docker']


def create_file_with_errors(file_num, error_type, n_rows=50):
    """Create a CSV file with specific error types"""
    
    data = {
        'experience_years': np.random.randint(0, 15, n_rows).astype(float),
        'skills': [random.choice(valid_skills) for _ in range(n_rows)],
        'education_level': [random.choice(valid_education) for _ in range(n_rows)],
        'location': [random.choice(valid_locations) for _ in range(n_rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Introduce specific errors
    if error_type == 'missing_column':
        # Missing required column
        df = df.drop('skills', axis=1)
        print(f"   ❌ Missing 'skills' column")
        
    elif error_type == 'missing_values':
        # Missing values in required columns
        null_indices = random.sample(range(len(df)), min(15, len(df)))
        df.loc[null_indices, 'experience_years'] = np.nan
        df.loc[null_indices[:10], 'education_level'] = np.nan
        print(f"   ❌ {len(null_indices)} null values in experience_years")
        print(f"   ❌ 10 null values in education_level")
        
    elif error_type == 'invalid_education':
        # Unknown education values
        invalid_indices = random.sample(range(len(df)), min(12, len(df)))
        invalid_edu_values = ['Elementary', 'Kindergarten', 'Some College', 'Certificate']
        for idx in invalid_indices:
            df.loc[idx, 'education_level'] = random.choice(invalid_edu_values)
        print(f"   ❌ {len(invalid_indices)} invalid education values")
        
    elif error_type == 'invalid_location':
        # Unknown location values
        invalid_indices = random.sample(range(len(df)), min(10, len(df)))
        invalid_locations = ['Mars', 'Atlantis', 'Narnia', 'Hogwarts']
        for idx in invalid_indices:
            df.loc[idx, 'location'] = random.choice(invalid_locations)
        print(f"   ❌ {len(invalid_indices)} invalid location values")
        
    elif error_type == 'negative_experience':
        # Negative experience years
        negative_indices = random.sample(range(len(df)), min(8, len(df)))
        df.loc[negative_indices, 'experience_years'] = np.random.randint(-10, -1, len(negative_indices))
        print(f"   ❌ {len(negative_indices)} negative experience values")
        
    elif error_type == 'excessive_experience':
        # Unrealistic experience (> 70 years)
        excessive_indices = random.sample(range(len(df)), min(6, len(df)))
        df.loc[excessive_indices, 'experience_years'] = np.random.randint(75, 150, len(excessive_indices))
        print(f"   ❌ {len(excessive_indices)} excessive experience values (>70)")
        
    elif error_type == 'string_in_numeric':
        # String values in numeric column
        string_indices = random.sample(range(len(df)), min(10, len(df)))
        string_values = ['five', 'ten', 'twenty', 'many', 'N/A', 'unknown']
        for idx in string_indices:
            df.loc[idx, 'experience_years'] = random.choice(string_values)
        print(f"   ❌ {len(string_indices)} string values in numeric column")
        
    elif error_type == 'empty_skills':
        # Empty skills field
        empty_indices = random.sample(range(len(df)), min(15, len(df)))
        df.loc[empty_indices, 'skills'] = ''
        print(f"   ❌ {len(empty_indices)} empty skills values")
        
    elif error_type == 'mixed_errors':
        # Multiple types of errors
        print(f"   ❌ Mixed errors:")
        # 1. Missing values
        null_idx = random.sample(range(len(df)), 5)
        df.loc[null_idx, 'experience_years'] = np.nan
        print(f"      • 5 null values")
        
        # 2. Invalid education
        inv_edu_idx = random.sample(range(len(df)), 4)
        for idx in inv_edu_idx:
            df.loc[idx, 'education_level'] = 'Elementary'
        print(f"      • 4 invalid education")
        
        # 3. Negative experience
        neg_idx = random.sample(range(len(df)), 3)
        df.loc[neg_idx, 'experience_years'] = -5
        print(f"      • 3 negative experience")
        
        # 4. Empty skills
        empty_idx = random.sample(range(len(df)), 6)
        df.loc[empty_idx, 'skills'] = ''
        print(f"      • 6 empty skills")
    
    elif error_type == 'clean':
        # Clean data - no errors
        print(f"   ✅ Clean data (no errors)")
    
    # Save file
    filename = f"test_batch_{file_num:03d}_{error_type}.csv"
    filepath = raw_data_folder / filename
    df.to_csv(filepath, index=False)
    
    return filename


# GENERATE TEST FILES

files_created = []

print("Creating test files with various error types:\n")

# 1. Clean file (no errors)
print("1. Clean data file:")
files_created.append(create_file_with_errors(1, 'clean', 60))

# 2. Missing column
print("\n2. Missing required column:")
files_created.append(create_file_with_errors(2, 'missing_column', 50))

# 3. Missing values
print("\n3. Missing values in columns:")
files_created.append(create_file_with_errors(3, 'missing_values', 50))

# 4. Invalid education
print("\n4. Invalid education values:")
files_created.append(create_file_with_errors(4, 'invalid_education', 50))

# 5. Invalid location
print("\n5. Invalid location values:")
files_created.append(create_file_with_errors(5, 'invalid_location', 50))

# 6. Negative experience
print("\n6. Negative experience years:")
files_created.append(create_file_with_errors(6, 'negative_experience', 50))

# 7. Excessive experience
print("\n7. Excessive experience years:")
files_created.append(create_file_with_errors(7, 'excessive_experience', 50))

# 8. String in numeric
print("\n8. String values in numeric column:")
files_created.append(create_file_with_errors(8, 'string_in_numeric', 50))

# 9. Empty skills
print("\n9. Empty skills values:")
files_created.append(create_file_with_errors(9, 'empty_skills', 50))

# 10. Mixed errors
print("\n10. Multiple error types:")
files_created.append(create_file_with_errors(10, 'mixed_errors', 60))

# Summary
print("\n" + "="*60)
print(f"✅ Created {len(files_created)} test files")
print("="*60)
print(f"\nFiles created in: {raw_data_folder}")
print("\nFiles:")
for f in files_created:
    print(f"   • {f}")

print("\n" + "="*60)
print("NEXT STEPS:")
print("="*60)
print("1. Deploy the updated DAG to Airflow")
print("2. Enable the 'data_ingestion_with_validation' DAG")
print("3. Watch it process files every 5 minutes")
print("4. Check reports in: 05-airflow-dags/reports/")
print("5. Check good_data/ and bad_data/ folders")
print("="*60)