import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
import json

class DataValidator:
    def __init__(self):
        """Initialize Great Expectations context"""
        self.context = gx.get_context()
        
    def validate_job_data(self, df, filename):
        """
        Validate job recommendation data using Great Expectations
        Returns: validation results dictionary
        """
        
        # Create expectation suite
        suite_name = "job_data_suite"
        
        try:
            self.context.get_expectation_suite(expectation_suite_name=suite_name)
        except:
            # Create new suite if doesn't exist
            suite = self.context.create_expectation_suite(
                expectation_suite_name=suite_name,
                overwrite_existing=True
            )
        
        # Create a validator
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="job_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": filename}
        )
        
        # Get or create datasource
        try:
            datasource = self.context.get_datasource("pandas_datasource")
        except:
            datasource_config = {
                "name": "pandas_datasource",
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    "runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            }
            self.context.add_datasource(**datasource_config)
        
        # Define expectations (data quality rules)
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Expectation 1: Required columns must exist
        validator.expect_table_columns_to_match_set(
            column_set=["experience_years", "skills", "education_level", "location"]
        )
        
        # Expectation 2: No missing values in required columns
        validator.expect_column_values_to_not_be_null("experience_years")
        validator.expect_column_values_to_not_be_null("skills")
        validator.expect_column_values_to_not_be_null("education_level")
        validator.expect_column_values_to_not_be_null("location")
        
        # Expectation 3: Experience years should be numeric
        validator.expect_column_values_to_be_of_type("experience_years", "float64")
        
        # Expectation 4: Experience years should be in valid range
        validator.expect_column_values_to_be_between(
            "experience_years", 
            min_value=0, 
            max_value=70
        )
        
        # Expectation 5: Education level should be in valid set
        validator.expect_column_values_to_be_in_set(
            "education_level",
            value_set=["High School", "Bachelor", "Master", "PhD", "Entry Level", "Mid Level", "Senior Level", "Expert"]
        )
        
        # Expectation 6: Skills should not be empty
        validator.expect_column_values_to_not_match_regex(
            "skills",
            regex=r"^\s*$"
        )
        
        # Expectation 7: No duplicate rows
        validator.expect_compound_columns_to_be_unique(
            column_list=["experience_years", "skills", "education_level", "location"]
        )
        
        # Run validation
        results = validator.validate()
        
        # Parse results
        validation_results = {
            "success": results.success,
            "statistics": results.statistics,
            "results": []
        }
        
        for result in results.results:
            validation_results["results"].append({
                "expectation_type": result.expectation_config.expectation_type,
                "success": result.success,
                "result": result.result
            })
        
        return validation_results
    
    def generate_html_report(self, validation_results, output_path):
        """Generate HTML report from validation results"""
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial; padding: 20px; background: #f5f5f5; }}
                .success {{ color: green; }}
                .failure {{ color: red; }}
                .container {{ background: white; padding: 20px; border-radius: 8px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                .stat {{ font-size: 24px; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Data Quality Validation Report</h1>
                <h2 class="{'success' if validation_results['success'] else 'failure'}">
                    {'✅ PASSED' if validation_results['success'] else '❌ FAILED'}
                </h2>
                
                <h3>Statistics</h3>
                <p><span class="stat">{validation_results['statistics']['evaluated_expectations']}</span> Expectations Evaluated</p>
                <p><span class="stat">{validation_results['statistics']['successful_expectations']}</span> Successful</p>
                <p><span class="stat">{validation_results['statistics']['unsuccessful_expectations']}</span> Failed</p>
                <p><span class="stat">{validation_results['statistics']['success_percent']:.1f}%</span> Success Rate</p>
                
                <h3>Detailed Results</h3>
                <table>
                    <tr>
                        <th>Expectation</th>
                        <th>Status</th>
                        <th>Details</th>
                    </tr>
        """
        
        for result in validation_results['results']:
            status = "✅ PASSED" if result['success'] else "❌ FAILED"
            status_class = "success" if result['success'] else "failure"
            
            html += f"""
                    <tr>
                        <td>{result['expectation_type']}</td>
                        <td class="{status_class}">{status}</td>
                        <td>{json.dumps(result['result'], indent=2) if result['result'] else 'N/A'}</td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        print(f"📄 HTML report generated: {output_path}")

# Usage example
if __name__ == "__main__":
    # Test with sample data
    sample_data = pd.DataFrame({
        'experience_years': [5.0, 3.0, -1.0],  # -1 will fail validation
        'skills': ['Python,SQL', 'Java', ''],  # Empty string will fail
        'education_level': ['Bachelor', 'Master', 'Elementary'],  # Elementary will fail
        'location': ['New York', 'Remote', 'Unknown']
    })
    
    validator = DataValidator()
    results = validator.validate_job_data(sample_data, "test.csv")
    
    print(f"\nValidation Success: {results['success']}")
    print(f"Success Rate: {results['statistics']['success_percent']:.1f}%")
    
    validator.generate_html_report(results, "reports/test_validation.html")