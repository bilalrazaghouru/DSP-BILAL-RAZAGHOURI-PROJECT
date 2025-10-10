from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Prediction

app = FastAPI(title="DSP FastAPI Backend")

def dummy_model(df: pd.DataFrame):
    # replace with real model
    return ["Recommended" if i % 2 == 0 else "Not Recommended" for i in range(len(df))]

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class PredictRequest(BaseModel):
    records: List[Dict[str, Any]]
    source: str = "webapp"
    model_version: str = "v0"

@app.post("/predict")
def predict(body: PredictRequest, db: Session = Depends(get_db)):
    df = pd.DataFrame(body.records)
    preds = dummy_model(df)
    for rec, pred in zip(body.records, preds):
        db.add(Prediction(source=body.source, model_version=body.model_version,
                          features=rec, prediction=pred))
    db.commit()
    return {"predictions": preds}

from datetime import datetime
from typing import Optional

@app.get("/past-predictions")
def past_predictions(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    source: Optional[str] = None,
    db: Session = Depends(get_db)
):
    q = db.query(Prediction)
    if start: q = q.filter(Prediction.created_at >= start)
    if end: q = q.filter(Prediction.created_at < end)
    if source and source != "all": q = q.filter(Prediction.source == source)
    rows = q.order_by(Prediction.created_at.desc()).limit(500).all()
    return [
        {
            "id": r.id,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "source": r.source,
            "model_version": r.model_version,
            "features": r.features,
            "prediction": r.prediction,
        } for r in rows
    ]
