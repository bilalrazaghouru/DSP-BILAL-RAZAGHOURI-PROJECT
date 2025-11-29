
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, ValidationError
from typing import List, Optional
import joblib
import numpy as np
import pandas as pd
import traceback
import logging
from pathlib import Path
import sys
import os
# Setup project root for imports
project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "01-database-setup"))

# Import from 01-database-setup folder
try:
    from schema import SessionLocal, Prediction
except ImportError:
    import importlib.util
    spec = importlib.util.spec_from_file_location("schema", project_root / "01-database-setup" / "schema.py")
    schema_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_module)
    SessionLocal = schema_module.SessionLocal
    Prediction = schema_module.Prediction

logger = logging.getLogger("uvicorn.error")

# Create the FastAPI app
app = FastAPI(title="Job Recommendation API", version="1.0")

# Health / root route
@app.get("/")
def home():
    return {"message": "API is working!"}

# Config: model / encoder paths
MODEL_PATH = project_root / "02-ml-model" / "models" / "job_recommender_model.pkl"
LE_EDU_PATH = project_root / "02-ml-model" / "models" / "education_encoder.pkl"
LE_LOC_PATH = project_root / "02-ml-model" / "models" / "location_encoder.pkl"
LE_JOB_PATH = project_root / "02-ml-model" / "models" / "job_encoder.pkl"
FEATURE_NAMES_PATH = project_root / "02-ml-model" / "models" / "feature_names.pkl"

print(f"🔍 Project root: {project_root}")
print(f"🔍 Model path: {MODEL_PATH}")
print(f"🔍 Model exists: {MODEL_PATH.exists()}")

# Load artifacts safely
def safe_load(path: Path):
    try:
        if path.exists():
            return joblib.load(path)
        else:
            logger.error(f"File not found: {path}")
            return None
    except Exception as e:
        logger.error(f"Could not load {path}: {e}")
        return None

model = safe_load(MODEL_PATH)
le_education = safe_load(LE_EDU_PATH)
le_location = safe_load(LE_LOC_PATH)
le_job = safe_load(LE_JOB_PATH)
feature_names = safe_load(FEATURE_NAMES_PATH)

if model is not None:
    logger.info("✅ Model loaded successfully!")
else:
    logger.warning("⚠️ Model not loaded - predictions will fail")

if feature_names is None:
    feature_names = ["experience_years", "skill_count", "education_encoded", "location_encoded"]

# Helpers
def get_classes_from_encoder(le):
    if le is None:
        return []
    try:
        return list(le.classes_)
    except Exception:
        try:
            return list(le)
        except Exception:
            return []

def safe_label_transform(le, value, fallback_index=0):
    try:
        classes = get_classes_from_encoder(le)
        if not classes:
            return 0
        # direct match (case-insensitive)
        for i, c in enumerate(classes):
            if str(value).strip().lower() == str(c).strip().lower():
                try:
                    return int(le.transform([c])[0])
                except Exception:
                    return i
        # substring/fuzzy match
        for i, c in enumerate(classes):
            if str(c).strip().lower() in str(value).strip().lower() or str(value).strip().lower() in str(c).strip().lower():
                try:
                    return int(le.transform([c])[0])
                except Exception:
                    return i
        # fallback
        fallback_value = classes[min(fallback_index, len(classes)-1)]
        try:
            return int(le.transform([fallback_value])[0])
        except Exception:
            return min(fallback_index, len(classes)-1)
    except Exception:
        logger.exception("safe_label_transform failed")
        return 0

def safe_predict_proba_batch(model_obj, X: pd.DataFrame):
    try:
        if model_obj is None:
            return np.array([0.5] * len(X))
        if hasattr(model_obj, "predict_proba"):
            proba = model_obj.predict_proba(X)
            if proba.ndim == 2:
                return np.max(proba, axis=1).astype(float)
            return np.ravel(proba).astype(float)
        if hasattr(model_obj, "decision_function"):
            df = model_obj.decision_function(X)
            dfv = np.array(df).ravel()
            prob = 1.0 / (1.0 + np.exp(-dfv))
            return prob.astype(float)
    except Exception:
        logger.exception("safe_predict_proba_batch failed")
    return np.array([0.5] * len(X))

# Background task for DB writes
def save_predictions_to_db(predictions_data: List[dict], source: str):
    """Save predictions to database in background"""
    db = SessionLocal()
    try:
        for pred_data in predictions_data:
            try:
                record = Prediction(
                    experience_years=pred_data["experience_years"],
                    skill_count=pred_data["skill_count"],
                    education_level=pred_data["education_level"],
                    location=pred_data["location"],
                    predicted_job=pred_data["predicted_job"],
                    prediction_probability=pred_data["probability"],
                    source=source
                )
                db.add(record)
            except Exception as e:
                logger.error(f"Failed to add prediction: {e}")
                continue
        
        db.commit()
        logger.info(f"✅ Saved {len(predictions_data)} predictions to database")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to save predictions: {e}")
    finally:
        db.close()

# Request/response schemas
class CandidateInput(BaseModel):
    experience_years: float
    skills: str
    education_level: str
    location: str

class PredictionResponse(BaseModel):
    predicted_job: str
    probability: float
    features_used: dict

# ULTRA-FAST Prediction endpoint

@app.post("/predict", response_model=List[PredictionResponse])
async def predict(candidates: List[CandidateInput], background_tasks: BackgroundTasks, source: str = "webapp"):
    """
    Ultra-fast batch prediction endpoint.
    Predictions are instant, database writes happen in background.
    """
    
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded on server.")

    try:
        # Build batch feature matrix - VECTORIZED for speed
        rows = []
        raw_inputs = []

        for c in candidates:
            parts = [p.strip() for p in str(c.skills).replace(";", ",").split(",") if p.strip()]
            skill_count = len(parts)

            education_encoded = safe_label_transform(le_education, c.education_level, fallback_index=0)
            location_encoded = safe_label_transform(le_location, c.location, fallback_index=0)

            row = []
            for fn in feature_names:
                fn_lower = fn.lower()
                if "experience" in fn_lower:
                    row.append(float(c.experience_years))
                elif "skill" in fn_lower:
                    row.append(int(skill_count))
                elif "educ" in fn_lower or "education" in fn_lower:
                    row.append(int(education_encoded))
                elif "loc" in fn_lower or "location" in fn_lower:
                    row.append(int(location_encoded))
                else:
                    row.append(0)

            rows.append(row)
            raw_inputs.append({
                "experience_years": c.experience_years,
                "skills": c.skills,
                "education_level": c.education_level,
                "location": c.location,
                "skill_count": skill_count
            })

        # SINGLE batch prediction - FAST!
        X = pd.DataFrame(rows, columns=feature_names)
        
        # Predict all at once
        preds_encoded = model.predict(X)
        probs = safe_predict_proba_batch(model, X)

        # Decode labels
        try:
            if le_job is not None:
                preds_decoded = le_job.inverse_transform(preds_encoded)
            else:
                preds_decoded = [str(p) for p in preds_encoded]
        except Exception:
            preds_decoded = [str(p) for p in preds_encoded]

        # Build responses (no DB writes here - FAST!)
        responses = []
        db_data = []
        
        for i in range(len(candidates)):
            inp = raw_inputs[i]
            pred_label = str(preds_decoded[i])
            prob = float(probs[i]) if i < len(probs) else 0.5

            responses.append(PredictionResponse(
                predicted_job=pred_label,
                probability=prob,
                features_used={
                    "experience_years": inp["experience_years"],
                    "skills": inp["skills"],
                    "education_level": inp["education_level"],
                    "location": inp["location"]
                }
            ))
            
            # Prepare data for background DB write
            db_data.append({
                "experience_years": inp["experience_years"],
                "skill_count": inp["skill_count"],
                "education_level": inp["education_level"],
                "location": inp["location"],
                "predicted_job": pred_label,
                "probability": prob
            })

        # Schedule DB writes to happen in background (non-blocking)
        background_tasks.add_task(save_predictions_to_db, db_data, source)
        
        # Return immediately - FAST!
        logger.info(f"✅ Predicted {len(responses)} candidates in batch")
        return responses

    except ValidationError as ve:
        raise HTTPException(status_code=422, detail=ve.errors())
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Prediction failed")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# Past predictions endpoint

from dateutil import parser as date_parser

@app.get("/past-predictions")
async def get_past_predictions(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: Optional[str] = "all",
    limit: int = 200,
    offset: int = 0
):
    db = SessionLocal()
    try:
        query = db.query(Prediction)

        if source and source.lower() != "all" and source.strip() != "":
            src = source.strip()
            try:
                query = query.filter(Prediction.source.ilike(f"%{src}%"))
            except Exception:
                query = query.filter(Prediction.source == src)

        if start_date and start_date.strip() != "":
            sd = date_parser.parse(start_date)
            query = query.filter(Prediction.timestamp >= sd)

        if end_date and end_date.strip() != "":
            ed = date_parser.parse(end_date)
            query = query.filter(Prediction.timestamp <= ed)

        results = (
            query.order_by(Prediction.timestamp.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

        out = []
        for r in results:
            out.append({
                "id": r.id,
                "experience_years": r.experience_years,
                "education_level": r.education_level,
                "location": r.location,
                "predicted_job": r.predicted_job,
                "probability": r.prediction_probability,
                "source": r.source,
                "timestamp": r.timestamp.isoformat() if hasattr(r.timestamp, "isoformat") else r.timestamp
            })
        return out

    finally:
        db.close()

# Local server run

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)