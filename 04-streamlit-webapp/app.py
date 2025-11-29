import sys
from pathlib import Path
from datetime import datetime, timedelta
import time

import joblib
import pandas as pd
import requests
import streamlit as st

# Paths & global config

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# FastAPI backend URL
API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="AI Job Recommender",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🎯 AI Job Recommendation System")

# Load encoder classes so UI matches backend

try:
    model_path = project_root / "02-ml-model" / "models"

    le_education = joblib.load(model_path / "education_encoder.pkl")
    le_location = joblib.load(model_path / "location_encoder.pkl")

    education_options = list(le_education.classes_)
    location_options = list(le_location.classes_)

    st.sidebar.success("✅ Models loaded successfully")
except Exception as e:
    st.sidebar.error(f"⚠️ Could not load encoders: {e}")
    # Fallback options
    education_options = ["High School", "Bachelor", "Master", "PhD"]
    location_options = [
        "New York",
        "San Francisco",
        "London",
        "Singapore",
        "Toronto",
        "Berlin",
        "Tokyo",
    ]

# Check API health 

try:
    health_check = requests.get(f"{API_URL}/", timeout=5)
    if health_check.status_code == 200:
        st.sidebar.success("✅ API Connected")
    else:
        st.sidebar.warning("⚠️ API responding but with issues")
except Exception:
    st.sidebar.warning("⚠️ API not responding (check if FastAPI is running)")
    # Don't show error on main page - it's annoying if API is actually working

# Sidebar nav
page = st.sidebar.radio("Choose a page", ["Make Predictions", "Past Predictions"])


# PAGE 1: MAKE PREDICTIONS

if page == "Make Predictions":
    st.header("Get Job Recommendations")

    tab1, tab2 = st.tabs(["Single Prediction", "Batch Prediction"])

    # SINGLE PREDICTION 
    with tab1:
        st.subheader("Enter Candidate Details")

        col1, col2 = st.columns(2)
        with col1:
            experience = st.number_input("Years of Experience", 0.0, 50.0, 2.0)
            education = st.selectbox("Education Level", education_options)
        with col2:
            skills = st.text_input(
                "Skills (comma-separated)", 
                "Python,SQL,Machine Learning"
            )
            location = st.selectbox("Preferred Location", location_options)

        if st.button("🔮 Get Recommendation", type="primary", use_container_width=True):
            data = [
                {
                    "experience_years": experience,
                    "skills": skills,
                    "education_level": education,
                    "location": location,
                }
            ]

            start_time = time.time()
            
            with st.spinner("🚀 Generating recommendation..."):
                try:
                    response = requests.post(
                        f"{API_URL}/predict", 
                        json=data, 
                        timeout=30
                    )

                    elapsed = time.time() - start_time

                    if response.status_code == 200:
                        result = response.json()[0]
                        
                        st.success(f"✅ Recommendation Generated in {elapsed:.2f}s!")

                        # Top metrics
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Recommended Job", result["predicted_job"])
                        col2.metric("Confidence", f"{result['probability'] * 100:.1f}%")
                        col3.metric("Response Time", f"{elapsed:.2f}s")

                        # 👉 NEW: Display as a dataframe along with the features used
                        single_df = pd.DataFrame([
                            {
                                **result["features_used"],
                                "predicted_job": result["predicted_job"],
                                "confidence": f"{result['probability'] * 100:.1f}%"
                            }
                        ])

                        st.subheader("📊 Prediction Details (as DataFrame)")
                        st.dataframe(single_df, use_container_width=True)

                        # Optional: keep the raw JSON view
                        with st.expander("📋 Raw Features Used (JSON)"):
                            st.json(result["features_used"])

                        st.balloons()
                    else:
                        st.error(f"❌ Error {response.status_code}: {response.text}")
                        
                except requests.exceptions.Timeout:
                    st.error("❌ Request timed out. The API took too long to respond.")
                except requests.exceptions.ConnectionError:
                    st.error("❌ Cannot connect to API. Make sure FastAPI is running on port 8000.")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

    # ULTRA-FAST BATCH PREDICTION 
    with tab2:
        st.subheader("📦 Upload CSV for Batch Predictions")
        
        # Info box with tips
        st.info("""
        📋 **Required columns:** experience_years, skills, education_level, location
        
        ⚡ **Performance tips:**
        - Files < 500 rows: Set batch size to 1000 for instant results
        - Files 500-2000 rows: Use batch size 500 (5-10 seconds)
        - Files > 2000 rows: Use batch size 200-300 (best for stability)
        """)

        # Batch size configuration
        col1, col2 = st.columns([3, 1])
        with col1:
            max_batch_size = st.slider(
                "⚙️ Batch size (rows per request)", 
                100, 1000, 500, 100,
                help="Larger batches = faster, but may timeout on slow connections"
            )
        with col2:
            st.metric("Batch Size", max_batch_size)
        
        uploaded = st.file_uploader("📁 Choose CSV file", type="csv")

        if uploaded:
            try:
                df = pd.read_csv(uploaded)
                
                # Show file stats
                col1, col2, col3 = st.columns(3)
                col1.metric("📊 Total Rows", len(df))
                col2.metric("📁 File Size", f"{uploaded.size / 1024:.1f} KB")
                
                # Calculate estimated time
                est_batches = (len(df) + max_batch_size - 1) // max_batch_size
                est_time = est_batches * 2  # ~2 seconds per batch
                col3.metric("⏱️ Est. Time", f"~{est_time}s")
                
                st.write("📄 **Preview:**")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Validate columns
                required_cols = ["experience_years", "skills", "education_level", "location"]
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    st.error(f"❌ Missing required columns: {missing_cols}")
                else:
                    st.success(f"✅ File validated successfully!")

                    # Big predict button
                    if st.button(
                        f"🚀 Predict All {len(df)} Rows", 
                        type="primary", 
                        use_container_width=True
                    ):
                        # Show processing plan
                        num_batches = (len(df) + max_batch_size - 1) // max_batch_size
                        
                        if num_batches > 1:
                            st.info(f"📦 Processing in {num_batches} batches of up to {max_batch_size} rows each")
                        else:
                            st.info(f"⚡ Processing all {len(df)} rows in one batch!")
                        
                        # Progress tracking
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        stats_placeholder = st.empty()
                        
                        all_results = []
                        failed_batches = []
                        total_start = time.time()
                        
                        try:
                            # Process in chunks
                            for i in range(0, len(df), max_batch_size):
                                batch_num = (i // max_batch_size) + 1
                                chunk = df.iloc[i:i + max_batch_size]
                                
                                status_text.info(f"⚙️ Processing batch {batch_num}/{num_batches} ({len(chunk)} rows)...")
                                batch_start = time.time()
                                
                                try:
                                    response = requests.post(
                                        f"{API_URL}/predict",
                                        json=chunk.to_dict("records"),
                                        timeout=max(60, len(chunk) * 0.3)  # Dynamic timeout
                                    )

                                    batch_time = time.time() - batch_start

                                    if response.status_code == 200:
                                        chunk_results = response.json()
                                        all_results.extend(chunk_results)
                                        
                                        # Show speed
                                        speed = len(chunk) / batch_time
                                        status_text.success(
                                            f"✅ Batch {batch_num}/{num_batches} complete "
                                            f"({len(chunk_results)} predictions in {batch_time:.2f}s "
                                            f"- {speed:.0f} predictions/sec)"
                                        )
                                    else:
                                        error_msg = f"Batch {batch_num}: HTTP {response.status_code}"
                                        status_text.error(f"❌ {error_msg}")
                                        failed_batches.append(batch_num)
                                        
                                except requests.exceptions.Timeout:
                                    status_text.error(
                                        f"❌ Batch {batch_num} timed out. "
                                        f"Try reducing batch size to {max_batch_size // 2}."
                                    )
                                    failed_batches.append(batch_num)
                                except Exception as e:
                                    status_text.error(f"❌ Batch {batch_num} failed: {str(e)}")
                                    failed_batches.append(batch_num)
                                
                                # Update progress
                                progress = min((i + max_batch_size) / len(df), 1.0)
                                progress_bar.progress(progress)
                                
                                # Show running stats
                                if all_results:
                                    stats_placeholder.info(
                                        f"📊 Progress: {len(all_results)}/{len(df)} predictions "
                                        f"({len(all_results)/len(df)*100:.1f}%)"
                                    )

                            # Calculate total time
                            total_time = time.time() - total_start
                            avg_speed = len(all_results) / total_time if total_time > 0 else 0

                            # Clear progress indicators
                            progress_bar.empty()
                            status_text.empty()
                            stats_placeholder.empty()

                            # Display final results
                            if all_results:
                                st.success(
                                    f"🎉 Successfully processed {len(all_results)}/{len(df)} predictions "
                                    f"in {total_time:.2f}s ({avg_speed:.0f} predictions/sec)!"
                                )
                                
                                if failed_batches:
                                    st.warning(
                                        f"⚠️ {len(failed_batches)} batch(es) failed: {failed_batches}. "
                                        f"Try reducing batch size or checking API logs."
                                    )
                                
                                # Create results DataFrame
                                results_df = pd.DataFrame(
                                    [
                                        {
                                            **r["features_used"],
                                            "predicted_job": r["predicted_job"],
                                            "confidence": f"{r['probability'] * 100:.1f}%",
                                        }
                                        for r in all_results
                                    ]
                                )

                                # Performance metrics
                                col1, col2, col3, col4 = st.columns(4)
                                col1.metric("✅ Total Predictions", len(results_df))
                                col2.metric("⏱️ Total Time", f"{total_time:.2f}s")
                                col3.metric("⚡ Speed", f"{avg_speed:.0f}/sec")
                                
                                if "predicted_job" in results_df.columns:
                                    col4.metric("🎯 Unique Jobs", results_df["predicted_job"].nunique())

                                # Display results table
                                st.subheader("📊 Prediction Results")
                                st.dataframe(results_df, use_container_width=True)

                                # Job distribution
                                if "predicted_job" in results_df.columns:
                                    st.subheader("📈 Job Distribution")
                                    job_counts = results_df["predicted_job"].value_counts().head(10)
                                    st.bar_chart(job_counts)

                                # Download button
                                csv = results_df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    "📥 Download Results as CSV",
                                    csv,
                                    f"predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    "text/csv",
                                    type="primary",
                                    use_container_width=True
                                )
                                
                                st.balloons()
                            else:
                                st.error(
                                    "❌ No predictions were generated. All batches failed. "
                                    "Check that FastAPI is running and try with a smaller batch size."
                                )
                                
                        except Exception as e:
                            st.error(f"❌ Fatal error during processing: {str(e)}")
                            
            except pd.errors.EmptyDataError:
                st.error("❌ The uploaded file is empty.")
            except pd.errors.ParserError:
                st.error("❌ Error parsing CSV file. Please check file format.")
            except Exception as e:
                st.error(f"❌ Error reading file: {str(e)}")

# PAGE 2: PAST PREDICTIONS

elif page == "Past Predictions":
    st.header("📊 View Past Predictions")

    col1, col2, col3 = st.columns(3)
    start_date = col1.date_input("Start Date", datetime.now() - timedelta(days=7))
    end_date = col2.date_input("End Date", datetime.now())
    source = col3.selectbox("Source", ["all", "webapp", "scheduled"])

    if st.button("🔍 Fetch Predictions", type="primary", use_container_width=True):
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source": source,
        }

        with st.spinner("Fetching predictions from database..."):
            try:
                response = requests.get(
                    f"{API_URL}/past-predictions", 
                    params=params, 
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()

                    if data:
                        df = pd.DataFrame(data)
                        st.success(f"✅ Found {len(df)} records")

                        # Statistics
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("📊 Total Predictions", len(df))

                        if "predicted_job" in df.columns:
                            col2.metric("🎯 Unique Jobs", df["predicted_job"].nunique())

                        if "probability" in df.columns:
                            avg_conf = df["probability"].mean() * 100
                            col3.metric("📈 Avg Confidence", f"{avg_conf:.1f}%")
                        
                        if "source" in df.columns:
                            col4.metric("📍 Sources", df["source"].nunique())

                        # Data table
                        st.subheader("📋 Prediction History")
                        st.dataframe(df, use_container_width=True)

                        # Job distribution chart
                        if "predicted_job" in df.columns:
                            st.subheader("📈 Top 10 Predicted Jobs")
                            job_counts = df["predicted_job"].value_counts().head(10)
                            st.bar_chart(job_counts)

                        # Timeline if timestamp available
                        if "timestamp" in df.columns:
                            st.subheader("⏱️ Predictions Over Time")
                            try:
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                daily_counts = df.set_index('timestamp').resample('D').size()
                                st.line_chart(daily_counts)
                            except Exception as e:
                                st.warning(f"Could not create timeline: {e}")

                        # Download button
                        csv = df.to_csv(index=False).encode("utf-8")
                        st.download_button(
                            "📥 Download Data",
                            csv,
                            f"predictions_{start_date}_to_{end_date}.csv",
                            "text/csv",
                            type="primary",
                            use_container_width=True
                        )
                    else:
                        st.info("ℹ️ No predictions found for the selected criteria.")
                else:
                    st.error(f"❌ Error {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                st.error("❌ Request timed out. Try a smaller date range.")
            except requests.exceptions.ConnectionError:
                st.error("❌ Cannot connect to API. Make sure FastAPI is running on port 8000.")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# Sidebar Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 System Info")
st.sidebar.info(
    f"""
**API URL:** {API_URL}  
**Version:** 2.0 (Optimized)  
**Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}

**Performance:**
- Single: <1 second
- Batch 100: ~2 seconds  
- Batch 500: ~5-10 seconds
"""
)

# Performance tip in sidebar
st.sidebar.markdown("### 💡 Performance Tips")
st.sidebar.success("""
**For best speed:**
- Files < 500 rows: Use batch 1000
- Files 500-2000: Use batch 500  
- Files > 2000: Use batch 200-300

**Faster predictions = larger batch size!**
""")
