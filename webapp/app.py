# webapp/app.py
import streamlit as st
import pandas as pd
import requests
from datetime import date

API_URL = "http://127.0.0.1:9000"

st.set_page_config(page_title="DSP Prediction Dashboard", layout="wide")
page = st.sidebar.radio("Select Page", ["Make Predictions", "Past Predictions"])

if page == "Make Predictions":
    st.header("Make Predictions")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Single record (JSON)")
        single_json = st.text_area(
            "Paste one JSON object matching your dataset columns",
            value='{"age": 30, "income": 45000, "city": "Paris"}',
            height=140,
        )
    with c2:
        st.subheader("Batch (CSV)")
        up = st.file_uploader("Upload CSV file", type=["csv"])

    if st.button("Predict"):
        records = []
        # Single
        if single_json.strip():
            try:
                rec = pd.read_json(single_json, typ="series").to_dict()
            except Exception:
                # try via json_normalize if user pasted a dict-like string
                rec = pd.json_normalize(pd.read_json(single_json, typ="series")).to_dict(orient="records")[0]
            records.append(rec)
        # Batch
        if up is not None:
            df = pd.read_csv(up)
            records += df.to_dict(orient="records")

        if not records:
            st.warning("No input records.")
        else:
            r = requests.post(f"{API_URL}/predict", json={"records": records, "source": "webapp"})
            if r.ok:
                preds = r.json().get("predictions", [])
                out = pd.DataFrame(records)
                if len(preds) == len(out):
                    out["prediction"] = preds
                st.success("Done")
                st.dataframe(out, use_container_width=True)
            else:
                st.error(f"API error: {r.status_code}\n{r.text}")

elif page == "Past Predictions":
    st.header("Past Predictions")
    col1, col2, col3 = st.columns(3)
    start = col1.date_input("Start date", value=date.today())
    end = col2.date_input("End date", value=date.today())
    source = col3.selectbox("Source", ["all", "webapp", "scheduled predictions"])

    if st.button("Load"):
        params = {}
        if start:
            params["start"] = pd.Timestamp(start).isoformat()
        if end:
            # add 1 day to include the selected end date
            params["end"] = (pd.Timestamp(end) + pd.Timedelta(days=1)).isoformat()
        if source:
            params["source"] = source

        r = requests.get(f"{API_URL}/past-predictions", params=params)
        if r.ok:
            data = r.json()
            if not data:
                st.info("No records found for the selected filters.")
            else:
                df = pd.DataFrame(data)
                st.dataframe(df, use_container_width=True)
        else:
            st.error(f"API error: {r.status_code}\n{r.text}")
