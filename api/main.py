from fastapi import FastAPI, UploadFile, File
import sqlite3
import pandas as pd
import random

app = FastAPI()

# Dummy ML model for now (later we can replace with real job recommendation model)
def dummy_model(data):
    return [random.choice(["Recommended", "Not Recommended"]) for _ in range(len(data))]

@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    # Read uploaded CSV into dataframe
    df = pd.read_csv(file.file)

    # Make dummy predictions
    preds = dummy_model(df)

    # Save predictions into SQLite DB
    conn = sqlite3.connect("db/app.db")
    cur = conn.cursor()
    for pred in preds:
        cur.execute("INSERT INTO predictions (filename, prediction) VALUES (?, ?)", (file.filename, pred))
    conn.commit()
    conn.close()

    return {"filename": file.filename, "predictions": preds}

@app.get("/health")
def health_check():
    return {"status": "ok"}
