# 06-data-validation/validate_data_gx.py
# FIXED - Great Expectations validator for GX 1.9.0

import great_expectations as gx
from great_expectations.core import ExpectationSuite
import pandas as pd
from pathlib import Path
from datetime import datetime

class GreatExpectationsValidator:
    """
    Data validator using Great Expectations (GX 1.9.0+)
    """
    
    def __init__(self):
        """Initialize Great Expectations context"""
        project_root = Path(__file__).parent.parent
        gx_dir = project_root / "gx"
        gx_dir.mkdir(exist_ok=True)
        
        # Initialize context
        self.context = gx.get_context(mode="file", project_root_dir=str(gx_dir))
        
        print(f"✅ Great Expectations context initialized")
        print(f"   GX Version: {gx.__version__}")
    
    def validate_job_data(self, df: pd.DataFrame, filename: str):
        """
        Validate job recommendation data
        
        Args:
            df: Pandas DataFrame with job data
            filename: Name of the file being validated
            
        Returns:
            Dictionary with validation results
        """
        
        print(f"\n🔍 Validating {filename}...")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        # Create or get pandas datasource
        datasource_name = "pandas_datasource"
        try:
            datasource = self.context.data_sources.add_pandas(name=datasource_name)
            print(f"   Created new datasource: {datasource_name}")
        except Exception:
            datasource = self.context.data_sources.get(datasource_name)
            print(f"   Using existing datasource: {datasource_name}")
        
        # Add or get dataframe asset
        asset_name = "job_data_asset"
        try:
            data_asset = datasource.add_dataframe_asset(name=asset_name)
            print(f"   Created new asset: {asset_name}")
        except Exception:
            data_asset = datasource.get_asset(asset_name)
            print(f"   Using existing asset: {asset_name}")
        
        # FIXED: Correct way to build batch request for GX 1.9.0
        batch_parameters = {"dataframe": df}
        batch_request = data_asset.build_batch_request(batch_parameters)
        
        print("   ✓ Batch request built")
        
        # Create or update expectation suite
        suite_name = "job_quality_suite"
        
        try:
            # Try to get existing suite first
            suite = self.context.suites.get(name=suite_name)
            # Delete it to start fresh
            self.context.suites.delete(name=suite_name)
            suite = self.context.suites.add(ExpectationSuite(name=suite_name))
            print(f"   Recreated suite: {suite_name}")
        except Exception:
            try:
                suite = self.context.suites.add(ExpectationSuite(name=suite_name))
                print(f"   Created new suite: {suite_name}")
            except Exception:
                suite = self.context.suites.get(name=suite_name)
                print(f"   Using existing suite: {suite_name}")
        
        # Get validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        print("\n📋 Adding expectations...")
        
        # ============================================
        # Define expectations
        # ============================================
        
        # 1. Required columns
        try:
            validator.expect_table_columns_to_match_set(
                column_set=["experience_years", "skills", "education_level", "location"]
            )
            print("   ✓ Column presence check")
        except Exception as e:
            print(f"   ⚠️  Column check error: {e}")
        
        # 2. No null values
        for col in ["experience_years", "skills", "education_level", "location"]:
            try:
                validator.expect_column_values_to_not_be_null(column=col)
            except Exception as e:
                print(f"   ⚠️  Null check error for {col}: {e}")
        print("   ✓ Null value checks")
        
        # 3. Experience range
        try:
            validator.expect_column_values_to_be_between(
                column="experience_years",
                min_value=0,
                max_value=70
            )
            print("   ✓ Experience range check")
        except Exception as e:
            print(f"   ⚠️  Range check error: {e}")
        
        # 4. Education values
        try:
            validator.expect_column_values_to_be_in_set(
                column="education_level",
                value_set=["High School", "Bachelor", "Master", "PhD", 
                          "Entry Level", "Mid Level", "Senior Level", "Expert"]
            )
            print("   ✓ Education values check")
        except Exception as e:
            print(f"   ⚠️  Education check error: {e}")
        
        # 5. Skills not empty
        try:
            validator.expect_column_values_to_not_match_regex(
                column="skills",
                regex=r"^\s*$"
            )
            print("   ✓ Skills not empty check")
        except Exception as e:
            print(f"   ⚠️  Skills check error: {e}")
        
        # 6. No duplicates
        try:
            validator.expect_compound_columns_to_be_unique(
                column_list=["experience_years", "skills", "education_level", "location"]
            )
            print("   ✓ Duplicate check")
        except Exception as e:
            print(f"   ⚠️  Duplicate check error: {e}")
        
        # Save suite
        try:
            validator.save_expectation_suite()
            print("   ✓ Suite saved")
        except Exception as e:
            print(f"   ⚠️  Save error: {e}")
        
        # ============================================
        # Run validation
        # ============================================
        
        print("\n⚡ Running validation...")
        
        try:
            validation_result = validator.validate()
        except Exception as e:
            print(f"❌ Validation error: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "filename": filename,
                "error": str(e)
            }
        
        # Parse results
        total = len(validation_result.results)
        successful = sum(1 for r in validation_result.results if r.success)
        failed = total - successful
        success_rate = (successful / total * 100) if total > 0 else 0
        
        results = {
            "success": validation_result.success,
            "filename": filename,
            "timestamp": datetime.now().isoformat(),
            "statistics": {
                "evaluated_expectations": total,
                "successful_expectations": successful,
                "unsuccessful_expectations": failed,
                "success_percent": round(success_rate, 2)
            },
            "results": []
        }
        
        # Extract details
        for r in validation_result.results:
            # Handle both old and new API
            try:
                exp_type = r.expectation_config.type if hasattr(r.expectation_config, 'type') else r.expectation_config.expectation_type
            except AttributeError:
                exp_type = str(type(r.expectation_config).__name__)
            
            try:
                column = r.expectation_config.kwargs.get("column", "N/A")
            except AttributeError:
                column = getattr(r.expectation_config, "column", "N/A")
            
            detail = {
                "expectation_type": exp_type,
                "success": r.success,
                "column": column
            }
            
            if not r.success and hasattr(r, 'result') and r.result:
                result_dict = r.result if isinstance(r.result, dict) else {}
                if "unexpected_count" in result_dict:
                    detail["unexpected_count"] = result_dict["unexpected_count"]
                if "unexpected_percent" in result_dict:
                    detail["unexpected_percent"] = round(result_dict["unexpected_percent"], 2)
            
            results["results"].append(detail)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"Validation {'PASSED ✅' if results['success'] else 'FAILED ❌'}")
        print(f"{'='*60}")
        print(f"Total Checks: {total}")
        print(f"Passed: {successful} ✅")
        print(f"Failed: {failed} ❌")
        print(f"Success Rate: {success_rate:.1f}%")
        print(f"{'='*60}\n")
        
        return results
    
    def generate_html_report(self, validation_results, output_path):
        """Generate HTML report"""
        
        if "error" in validation_results:
            html = f"""
<!DOCTYPE html>
<html>
<head><title>Validation Error</title></head>
<body>
    <h1>❌ Validation Error</h1>
    <p><strong>File:</strong> {validation_results['filename']}</p>
    <p><strong>Error:</strong> {validation_results['error']}</p>
</body>
</html>
            """
        else:
            success = validation_results['success']
            stats = validation_results['statistics']
            color = "#4CAF50" if success else "#f44336"
            status = "PASSED ✅" if success else "FAILED ❌"
            
            html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Great Expectations Report</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px;
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        .header {{
            background: {color};
            color: white;
            padding: 40px;
            text-align: center;
        }}
        .header h1 {{ font-size: 2.5rem; margin-bottom: 10px; }}
        .header .status {{ font-size: 1.8rem; font-weight: bold; margin-top: 20px; }}
        .content {{ padding: 40px; }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin: 40px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 15px;
            text-align: center;
            box-shadow: 0 10px 25px rgba(0,0,0,0.2);
        }}
        .stat-card .number {{ font-size: 3rem; font-weight: bold; display: block; margin-bottom: 10px; }}
        .stat-card .label {{ font-size: 0.9rem; opacity: 0.9; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 30px; box-shadow: 0 5px 15px rgba(0,0,0,0.1); }}
        th {{ background: {color}; color: white; padding: 15px; text-align: left; font-weight: 600; }}
        td {{ padding: 15px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f8f9fa; }}
        .badge {{ display: inline-block; padding: 5px 15px; border-radius: 20px; font-size: 0.85rem; font-weight: bold; }}
        .badge-success {{ background: #d4edda; color: #155724; }}
        .badge-danger {{ background: #f8d7da; color: #721c24; }}
        h2 {{ color: #333; margin: 30px 0 20px 0; padding-bottom: 10px; border-bottom: 3px solid {color}; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Great Expectations Report</h1>
            <div class="status">{status}</div>
        </div>
        <div class="content">
            <p><strong>File:</strong> {validation_results['filename']}</p>
            <p><strong>Time:</strong> {validation_results['timestamp']}</p>
            
            <h2>📈 Statistics</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <span class="number">{stats['evaluated_expectations']}</span>
                    <span class="label">Total Checks</span>
                </div>
                <div class="stat-card">
                    <span class="number">{stats['successful_expectations']}</span>
                    <span class="label">Passed ✅</span>
                </div>
                <div class="stat-card">
                    <span class="number">{stats['unsuccessful_expectations']}</span>
                    <span class="label">Failed ❌</span>
                </div>
                <div class="stat-card">
                    <span class="number">{stats['success_percent']:.1f}%</span>
                    <span class="label">Success Rate</span>
                </div>
            </div>
            
            <h2>🔍 Details</h2>
            <table>
                <thead>
                    <tr>
                        <th>Expectation</th>
                        <th>Column</th>
                        <th>Status</th>
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody>
            """
            
            for r in validation_results['results']:
                badge = '<span class="badge badge-success">✅ PASSED</span>' if r['success'] else '<span class="badge badge-danger">❌ FAILED</span>'
                
                details = []
                if 'unexpected_count' in r:
                    details.append(f"Errors: {r['unexpected_count']}")
                if 'unexpected_percent' in r:
                    details.append(f"({r['unexpected_percent']}%)")
                details_str = " ".join(details) if details else "OK"
                
                html += f"""
                        <tr>
                            <td>{r['expectation_type']}</td>
                            <td><code>{r['column']}</code></td>
                            <td>{badge}</td>
                            <td>{details_str}</td>
                        </tr>
                """
            
            html += """
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
            """
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"📄 Report saved: {output_path}")


# Test
if __name__ == "__main__":
    print("="*60)
    print("GREAT EXPECTATIONS TEST (GX 1.9.0)")
    print("="*60)
    
    # Test data with errors
    test_data = pd.DataFrame({
        'experience_years': [5.0, 3.0, -2.0, 150.0, 7.0],  # -2 and 150 fail
        'skills': ['Python,SQL', 'Java', '', 'ML', 'AWS'],  # Empty fails
        'education_level': ['Bachelor', 'Master', 'Elementary', 'PhD', 'Bachelor'],  # Elementary fails
        'location': ['New York', 'Remote', 'SF', 'London', 'Toronto']
    })
    
    print(f"\n📊 Test data: {len(test_data)} rows\n")
    
    try:
        validator = GreatExpectationsValidator()
        results = validator.validate_job_data(test_data, "test.csv")
        
        if "error" not in results:
            validator.generate_html_report(results, "reports/gx_report.html")
            print("\n✅ Complete! Open reports/gx_report.html")
        else:
            print(f"\n❌ Validation failed: {results['error']}")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()