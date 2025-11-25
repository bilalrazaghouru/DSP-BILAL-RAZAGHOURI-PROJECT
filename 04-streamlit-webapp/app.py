import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import joblib

API_URL = "http://localhost:8000" 

st.set_page_config(page_title="AI Job Recommender", layout="wide")
st.title("AI Job Recommendation System")

le_education = joblib.load("models/education_encoder.pkl")
le_location = joblib.load("models/location_encoder.pkl")

education_options = list(le_education.classes_)
location_options = list(le_location.classes_)

page = st.sidebar.radio("Choose a page", ["Make Predictions", "Past Predictions"])

if page == "Make Predictions":
    st.header("Get Job Recommendations")
    
    tab1, tab2 = st.tabs(["Single Prediction", "Batch Prediction"])
    
    with tab1:
        st.subheader("Enter Candidate Details")

        col1, col2 = st.columns(2)
        with col1:
            experience = st.number_input("Years of Experience", 0.0, 50.0, 2.0)
            education = st.selectbox("Education Level", education_options)
        with col2:
            skills = st.text_input("Skills (comma-separated)", "Python,SQL,Machine Learning")
            location = st.selectbox("Preferred Location", location_options)

        if st.button("Get Recommendation"):
            data = [{
                "experience_years": experience,
                "skills": skills,
                "education_level": education,
                "location": location
            }]

            with st.spinner("Generating recommendation..."):
                response = requests.post(f"{API_URL}/predict", json=data)

                if response.status_code == 200:
                    result = response.json()[0]
                    st.success("Recommendation Generated!")

                    col1, col2 = st.columns(2)
                    col1.metric("Recommended Job", result["predicted_job"])
                    col2.metric("Confidence", f"{result['probability']*100:.1f}%")

                    st.json(result["features_used"])
                else:
                    st.error(f"Error: {response.text}")


    with tab2:
        st.subheader("Upload CSV for Batch Predictions")
        st.info("The CSV must have: experience_years, skills, education_level, location")

        uploaded = st.file_uploader("Choose CSV file", type="csv")

        if uploaded:
            df = pd.read_csv(uploaded)
            st.write("📄 Preview:", df.head())

            if st.button("Predict All"):
                with st.spinner(f"Processing {len(df)} candidates..."):
                    response = requests.post(f"{API_URL}/predict", json=df.to_dict("records"))

                    if response.status_code == 200:
                        results = response.json()
                        results_df = pd.DataFrame([{
                            **r["features_used"],
                            "predicted_job": r["predicted_job"],
                            "confidence": f"{r['probability']*100:.1f}%"
                        } for r in results])

                        st.success(f"Processed {len(results)} predictions!")
                        st.dataframe(results_df, use_container_width=True)

                        csv = results_df.to_csv(index=False).encode("utf-8")
                        st.download_button("📥 Download Results", csv, "predictions.csv", "text/csv")
                    else:
                        st.error(f"Error: {response.text}")


elif page == "Past Predictions":
    st.header("📊 View Past Predictions")

    col1, col2, col3 = st.columns(3)
    start_date = col1.date_input("Start Date", datetime.now() - timedelta(days=7))
    end_date = col2.date_input("End Date", datetime.now())
    source = col3.selectbox("Source", ["all", "webapp", "scheduled"])

    if st.button("Fetch"):
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source": source
        }

        response = requests.get(f"{API_URL}/past-predictions", params=params)

        if response.status_code == 200:
            data = response.json()

            if data:
                df = pd.DataFrame(data)
                st.success(f"Found {len(df)} records")
                st.dataframe(df, use_container_width=True)

                col1, col2, col3 = st.columns(3)
