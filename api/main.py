# api/main.py
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
import pandas as pd

from database import SessionLocal
from models import Prediction

app = FastAPI(title="DSP FastAPI Backend")

# Allow Streamlit to call us locally
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------- Dummy model logic (replace with your own) ----------
def dummy_model(df: pd.DataFrame) -> List[str]:
    # Example rule-based output — edit to match your columns
    # If your dataset has columns like "age" and "income", adjust below.
    if "age" in df.columns and "income" in df.columns:
        return [
            "Approved" if (row["age"] or 0) >= 25 and (row["income"] or 0) >= 30000 else "Rejected"
            for _, row in df.fillna(0).iterrows()
        ]
    # Fallback: alternate labels
    return ["Recommended" if i % 2 == 0 else "Not Recommended" for i in range(len(df))]
# ------------------------------------------------------------

class PredictRequest(BaseModel):
    records: List[Dict[str, Any]]
    source: str = "webapp"
    model_version: str = "v0"

@app.post("/predict")
def predict(body: PredictRequest, db: Session = Depends(get_db)):
    if not body.records:
        return {"predictions": []}
    df = pd.DataFrame(body.records)
    preds = dummy_model(df)
    for rec, pred in zip(body.records, preds):
        db.add(Prediction(
            source=body.source,
            model_version=body.model_version,
            features=rec,
            prediction=str(pred),
        ))
    db.commit()
    return {"predictions": [str(p) for p in preds]}

from datetime import datetime

@app.get("/past-predictions")
def past_predictions(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    source: Optional[str] = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(Prediction)
    if start:
        q = q.filter(Prediction.created_at >= start)
    if end:
        q = q.filter(Prediction.created_at < end)
    if source and source != "all":
        q = q.filter(Prediction.source == source)
    rows = q.order_by(Prediction.created_at.desc()).limit(limit).all()
    return [
        {
            "id": r.id,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "source": r.source,
            "model_version": r.model_version,
            "features": r.features,
            "prediction": r.prediction,
        }
        for r in rows
    ]
