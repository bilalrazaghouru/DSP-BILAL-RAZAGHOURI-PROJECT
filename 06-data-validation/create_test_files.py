# 06-data-validation/create_test_files.py
import pandas as pd
import numpy as np
import random
import os
from pathlib import Path

# Get project root and create data folder
project_root = Path(__file__).parent.parent.resolve()
raw_data_folder = project_root / 'data' / 'raw-data'

# Create folder if it doesn't exist
raw_data_folder.mkdir(parents=True, exist_ok=True)

print(f"📁 Creating test files in: {raw_data_folder}")
print("="*60)

# Create 10 test files with varying quality
for i in range(10):
    n_rows = random.randint(50, 100)
    
    data = {
        'experience_years': np.random.randint(0, 15, n_rows).astype(float),
        'skills': [random.choice(['Python,SQL', 'Java,Spring', 'JS,React', 'Data,ML', 'Cloud,AWS']) for _ in range(n_rows)],
        'education_level': [random.choice(['Bachelor', 'Master', 'PhD']) for _ in range(n_rows)],
        'location': [random.choice(['New York', 'San Francisco', 'Remote', 'London', 'Toronto']) for _ in range(n_rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Add errors to some files (70% of files have errors)
    if i < 7:
        error_indices = random.sample(range(len(df)), min(random.randint(5, 15), len(df)))
        
        # Missing values
        for idx in error_indices[:len(error_indices)//4]:
            df.loc[idx, 'experience_years'] = np.nan
        
        # Out of range values
        for idx in error_indices[len(error_indices)//4:len(error_indices)//2]:
            df.loc[idx, 'experience_years'] = random.choice([-5, 150])
        
        # Empty skills
        for idx in error_indices[len(error_indices)//2:3*len(error_indices)//4]:
            df.loc[idx, 'skills'] = ''
        
        # Invalid education
        for idx in error_indices[3*len(error_indices)//4:]:
            df.loc[idx, 'education_level'] = 'Elementary'
        
        status = "with errors ❌"
    else:
        status = "clean ✅"
    
    filename = raw_data_folder / f'batch_{i+1:03d}.csv'
    df.to_csv(filename, index=False)
    print(f"✅ Created batch_{i+1:03d}.csv ({len(df)} rows, {status})")

print("="*60)
print(f"✅ Created 10 test files in: {raw_data_folder}")
print(f"\nNext steps:")
print(f"1. Start Airflow: docker compose up -d")
print(f"2. DAG will process files every 5 minutes")
print(f"3. Check reports in: {project_root / 'reports'}")