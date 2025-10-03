import streamlit as st
import requests
import pandas as pd

st.title("Job Recommendation Demo App")

uploaded_file = st.file_uploader("Upload a CSV file for prediction")

if uploaded_file is not None:
    files = {"file": uploaded_file}
    response = requests.post("http://127.0.0.1:8000/predict", files=files)

    if response.status_code == 200:
        result = response.json()
        st.write("### Predictions:")
        df = pd.DataFrame({"Prediction": result["predictions"]})
        st.dataframe(df)
    else:
        st.error("Error calling API")
