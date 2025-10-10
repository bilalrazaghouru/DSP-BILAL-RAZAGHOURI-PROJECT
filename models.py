# models.py
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from database import Base

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    source = Column(String, nullable=False)              # "webapp" | "scheduled predictions"
    model_version = Column(String, nullable=False, default="v0")
    features = Column(JSONB, nullable=False)             # full input row
    prediction = Column(String, nullable=False)          # your model output (stringify if needed)
