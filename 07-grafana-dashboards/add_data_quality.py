import sys
sys.path.append('.')
from src.database.schema import SessionLocal, DataQuality
from datetime import datetime, timedelta
import random
import json

db = SessionLocal()

print("Adding data quality records...")

# Generate 30 data quality records over last 2 hours
for i in range(30):
    minutes_ago = random.randint(0, 120)
    timestamp = datetime.now() - timedelta(minutes=minutes_ago)
    
    # Random error counts
    errors = {
        'missing_values': random.randint(0, 15),
        'invalid_values': random.randint(0, 10),
        'out_of_range': random.randint(0, 5),
        'duplicates': random.randint(0, 3),
        'type_errors': random.randint(0, 8),
        'unknown_categories': random.randint(0, 6)
    }
    
    # Calculate totals
    total_rows = random.randint(80, 150)
    invalid_rows = random.randint(5, 50)
    valid_rows = total_rows - invalid_rows
    
    # Determine criticality
    error_rate = invalid_rows / total_rows
    if error_rate > 0.5:
        criticality = 'high'
    elif error_rate > 0.2:
        criticality = 'medium'
    else:
        criticality = 'low'
    
    # Create record
    dq = DataQuality(
        filename=f'batch_{i+1:03d}.csv',
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        error_types=json.dumps(errors),
        criticality=criticality,
        timestamp=timestamp
    )
    db.add(dq)
    
    if (i + 1) % 10 == 0:
        print(f"  Added {i+1} records...")

db.commit()
db.close()

print(f"\n✅ Added 30 data quality records!")
print("Now refresh your Grafana dashboard!")