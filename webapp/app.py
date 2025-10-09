import streamlit as st
import pandas as pd
import requests
from io import BytesIO

# -------------------------------
# CONFIG
# -------------------------------
API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="DSP Prediction Dashboard", layout="wide")
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select Page", ["Make Predictions", "Past Predictions"])

# -------------------------------
# PAGE 1 — Make Predictions
# -------------------------------
if page == "Make Predictions":
    st.header("🤖 Make Predictions")

    st.subheader("Batch Prediction (CSV Upload)")
    uploaded = st.file_uploader("Upload a CSV file", type=["csv"])

    if uploaded is not None:
        st.write("Preview of uploaded file:")
        df = pd.read_csv(uploaded)
        st.dataframe(df.head())

        if st.button("Run Prediction"):
            try:
                # Send the file to FastAPI as multipart/form-data
                files = {"file": (uploaded.name, uploaded.getvalue(), "text/csv")}
                response = requests.post(f"{API_URL}/predict", files=files)

                if response.status_code == 200:
                    result = response.json()
                    st.success("✅ Prediction successful!")
                    st.write("Predictions:")
                    preds = pd.DataFrame(result["predictions"], columns=["Prediction"])
                    st.dataframe(preds)
                else:
                    st.error(f"❌ API Error: {response.text}")
            except Exception as e:
                st.error(f"⚠️ Connection error: {e}")

# -------------------------------
# PAGE 2 — Past Predictions
# -------------------------------
elif page == "Past Predictions":
    st.header("📜 Past Predictions")

    if st.button("Load Past Predictions"):
        try:
            response = requests.get(f"{API_URL}/past-predictions")
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = pd.DataFrame(data)
                    st.dataframe(df)
                else:
                    st.info("No past predictions found.")
            else:
                st.error(f"❌ Error: {response.text}")
        except Exception as e:
            st.error(f"⚠️ Could not connect to API: {e}")
