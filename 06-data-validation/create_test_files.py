import pandas as pd
import numpy as np
import random
import os

os.makedirs('data/raw-data', exist_ok=True)

# Create 10 test files with varying quality
for i in range(10):
    n_rows = random.randint(50, 100)
    
    data = {
        'experience_years': np.random.randint(0, 15, n_rows).astype(float),
        'skills': [random.choice(['Python,SQL', 'Java,Spring', 'JS,React', 'Data,ML']) for _ in range(n_rows)],
        'education_level': [random.choice(['Bachelor', 'Master', 'PhD']) for _ in range(n_rows)],
        'location': [random.choice(['New York', 'SF', 'Remote']) for _ in range(n_rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Add errors to some files
    if i < 7:  # 70% of files have errors
        # Add various errors
        error_indices = random.sample(range(len(df)), min(random.randint(5, 15), len(df)))
        
        for idx in error_indices[:len(error_indices)//4]:
            df.loc[idx, 'experience_years'] = np.nan  # Missing values
        
        for idx in error_indices[len(error_indices)//4:len(error_indices)//2]:
            df.loc[idx, 'experience_years'] = random.choice([-5, 150])  # Out of range
        
        for idx in error_indices[len(error_indices)//2:3*len(error_indices)//4]:
            df.loc[idx, 'skills'] = ''  # Empty
        
        for idx in error_indices[3*len(error_indices)//4:]:
            df.loc[idx, 'education_level'] = 'Elementary'  # Invalid
    
    filename = f'data/raw-data/batch_{i+1:03d}.csv'
    df.to_csv(filename, index=False)
    print(f"✅ Created {filename} ({len(df)} rows)")

print(f"\n✅ Created 10 test files in data/raw-data/")