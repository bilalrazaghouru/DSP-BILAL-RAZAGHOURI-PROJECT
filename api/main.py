from fastapi import FastAPI, UploadFile, File
import pandas as pd
import random
from database import SessionLocal
from models import Student  # change later if you add other models

app = FastAPI(title="DSP FastAPI Backend")

# Dummy ML model
def dummy_model(data):
    return [random.choice(["Recommended", "Not Recommended"]) for _ in range(len(data))]

@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    preds = dummy_model(df)

    db = SessionLocal()
    for i, pred in enumerate(preds):
        record = Student(name=f"Student_{i}", course=pred)
        db.add(record)
    db.commit()
    db.close()

    return {"filename": file.filename, "predictions": preds}

@app.get("/past-predictions")
def get_past_predictions():
    db = SessionLocal()
    records = db.query(Student).all()
    db.close()
    return [{"id": r.id, "name": r.name, "course": r.course} for r in records]

@app.get("/health")
def health_check():
    return {"status": "ok", "message": "Connected to PostgreSQL 🚀"}
