import pandas as pd
import json
from datetime import datetime

class DataValidator:
    """Simple data validator without Great Expectations CLI"""
    
    def __init__(self):
        self.validation_rules = {
            'required_columns': ['experience_years', 'skills', 'education_level', 'location'],
            'valid_education': ['High School', 'Bachelor', 'Master', 'PhD', 'Entry Level', 'Mid Level', 'Senior Level', 'Expert'],
            'experience_min': 0,
            'experience_max': 70
        }
    
    def validate_job_data(self, df, filename):
        """Validate job recommendation data"""
        
        errors = []
        total_rows = len(df)
        
        # Rule 1: Check required columns exist
        missing_cols = [col for col in self.validation_rules['required_columns'] if col not in df.columns]
        if missing_cols:
            errors.append({
                'type': 'missing_columns',
                'severity': 'high',
                'message': f"Missing required columns: {missing_cols}",
                'count': len(missing_cols)
            })
        
        if not missing_cols:  # Only check further if columns exist
            
            # Rule 2: Check for missing values
            for col in self.validation_rules['required_columns']:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    errors.append({
                        'type': 'missing_values',
                        'severity': 'high',
                        'message': f"Missing values in {col}",
                        'count': int(missing_count)
                    })
            
            # Rule 3: Check experience_years is numeric
            if 'experience_years' in df.columns:
                non_numeric = pd.to_numeric(df['experience_years'], errors='coerce').isna() & df['experience_years'].notna()
                if non_numeric.sum() > 0:
                    errors.append({
                        'type': 'type_error',
                        'severity': 'high',
                        'message': "Non-numeric values in experience_years",
                        'count': int(non_numeric.sum())
                    })
            
            # Rule 4: Check experience_years range
            if 'experience_years' in df.columns:
                numeric_exp = pd.to_numeric(df['experience_years'], errors='coerce')
                out_of_range = ((numeric_exp < self.validation_rules['experience_min']) | 
                              (numeric_exp > self.validation_rules['experience_max']))
                if out_of_range.sum() > 0:
                    errors.append({
                        'type': 'out_of_range',
                        'severity': 'medium',
                        'message': f"Experience years outside valid range (0-70)",
                        'count': int(out_of_range.sum())
                    })
            
            # Rule 5: Check valid education levels
            if 'education_level' in df.columns:
                invalid_edu = ~df['education_level'].isin(self.validation_rules['valid_education'])
                if invalid_edu.sum() > 0:
                    errors.append({
                        'type': 'invalid_category',
                        'severity': 'medium',
                        'message': "Invalid education level values",
                        'count': int(invalid_edu.sum())
                    })
            
            # Rule 6: Check for empty strings in skills
            if 'skills' in df.columns:
                empty_skills = df['skills'].str.strip().str.len() == 0
                if empty_skills.sum() > 0:
                    errors.append({
                        'type': 'empty_value',
                        'severity': 'medium',
                        'message': "Empty skills field",
                        'count': int(empty_skills.sum())
                    })
            
            # Rule 7: Check for duplicates
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                errors.append({
                    'type': 'duplicates',
                    'severity': 'low',
                    'message': "Duplicate rows found",
                    'count': int(duplicates)
                })
        
        # Calculate statistics
        total_errors = sum(error['count'] for error in errors)
        success = len(errors) == 0
        
        return {
            'success': success,
            'filename': filename,
            'total_rows': total_rows,
            'total_errors': total_errors,
            'error_types': len(errors),
            'errors': errors,
            'timestamp': datetime.now().isoformat()
        }
    
    def generate_html_report(self, validation_results, output_path):
        """Generate HTML report from validation results"""
        
        results = validation_results
        color = 'green' if results['success'] else 'red'
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Report</title>
            <style>
                body {{ font-family: Arial; padding: 20px; background: #f5f5f5; }}
                .container {{ background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .header {{ border-bottom: 3px solid {color}; padding-bottom: 20px; }}
                .success {{ color: green; }}
                .failure {{ color: red; }}
                .stat {{ font-size: 48px; font-weight: bold; margin: 10px 0; }}
                .stat-label {{ font-size: 14px; color: #666; }}
                .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin: 30px 0; }}
                .stat-box {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                .severity-high {{ background-color: #ffebee; }}
                .severity-medium {{ background-color: #fff3e0; }}
                .severity-low {{ background-color: #e8f5e9; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 Data Quality Validation Report</h1>
                    <h2 class="{'success' if results['success'] else 'failure'}">
                        {'✅ ALL CHECKS PASSED' if results['success'] else '❌ VALIDATION FAILED'}
                    </h2>
                    <p><strong>File:</strong> {results['filename']}</p>
                    <p><strong>Timestamp:</strong> {results['timestamp']}</p>
                </div>
                
                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="stat">{results['total_rows']}</div>
                        <div class="stat-label">Total Rows</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat">{results['total_errors']}</div>
                        <div class="stat-label">Total Errors</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat">{results['error_types']}</div>
                        <div class="stat-label">Error Types</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat">{100 - (results['total_errors']/results['total_rows']*100 if results['total_rows'] > 0 else 0):.1f}%</div>
                        <div class="stat-label">Data Quality</div>
                    </div>
                </div>
                
                <h3>🔍 Detected Issues</h3>
        """
        
        if results['errors']:
            html += """
                <table>
                    <tr>
                        <th>Severity</th>
                        <th>Error Type</th>
                        <th>Description</th>
                        <th>Count</th>
                    </tr>
            """
            
            for error in results['errors']:
                html += f"""
                    <tr class="severity-{error['severity']}">
                        <td>{error['severity'].upper()}</td>
                        <td>{error['type']}</td>
                        <td>{error['message']}</td>
                        <td><strong>{error['count']}</strong></td>
                    </tr>
                """
            
            html += "</table>"
        else:
            html += "<p style='color: green; font-size: 18px;'>✅ No issues detected! Data quality is excellent.</p>"
        
        html += """
            </div>
        </body>
        </html>
        """
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"📄 HTML report generated: {output_path}")


# Test script
if __name__ == "__main__":
    print("Testing Data Validator...")
    
    # Create test data with errors
    test_data = pd.DataFrame({
        'experience_years': [5.0, 3.0, -2.0, 150.0, 'five'],  # Negative, too high, string
        'skills': ['Python,SQL', 'Java', '', 'ML', 'Data'],  # Empty string
        'education_level': ['Bachelor', 'Master', 'Elementary', 'PhD', 'Bachelor'],  # Invalid
        'location': ['New York', 'Remote', 'Unknown', 'SF', 'Austin']
    })
    
    print("\nTest data:")
    print(test_data)
    print("\nRunning validation...")
    
    validator = DataValidator()
    results = validator.validate_job_data(test_data, "test_errors.csv")
    
    print(f"\n{'='*50}")
    print(f"Validation Success: {results['success']}")
    print(f"Total Rows: {results['total_rows']}")
    print(f"Total Errors: {results['total_errors']}")
    print(f"Error Types: {results['error_types']}")
    print(f"{'='*50}\n")
    
    print("Errors detected:")
    for error in results['errors']:
        print(f"  • [{error['severity'].upper()}] {error['message']}: {error['count']} occurrences")
    
    # Generate report
    import os
    os.makedirs('reports', exist_ok=True)
    validator.generate_html_report(results, "reports/validation_test.html")
    print("\n✅ Done! Open reports/validation_test.html to view the report")