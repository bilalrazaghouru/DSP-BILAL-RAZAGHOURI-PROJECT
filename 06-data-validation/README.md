# Data Quality Validation
**Developer:** [Member 3 Name]

## Validation Rules (7 Checks)

1. **Missing Columns** - Required columns must exist
2. **Null Values** - No missing values in required fields
3. **Type Errors** - Data types must match schema
4. **Range Validation** - Experience 0-70 years
5. **Category Validation** - Education levels must be valid
6. **Empty Strings** - Skills field cannot be empty
7. **Duplicates** - No duplicate rows

## Files

### validate_data.py (10,521 bytes)
Custom validator class with:
- 7 validation rules
- HTML report generation
- Criticality assessment (high/medium/low)
- Error statistics

### create_test_files.py (1,633 bytes)
Generates test CSV files with intentional errors for testing validation pipeline.

### split_dataset.py (1,512 bytes)
Splits main dataset into multiple files for batch processing.

## Usage
\\\ash
python validate_data.py
\\\

## Output
- HTML validation reports in \eports/\ folder
- Statistics saved to database
- Data split into good_data/bad_data folders
