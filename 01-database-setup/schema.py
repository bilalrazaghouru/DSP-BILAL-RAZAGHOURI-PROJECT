from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

# --- Table 1: Predictions ---
class Prediction(Base):
    __tablename__ = 'predictions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    experience_years = Column(Float)
    skill_count = Column(Integer)
    education_level = Column(String(50))
    location = Column(String(100))
    predicted_job = Column(String(200))
    prediction_probability = Column(Float)
    source = Column(String(20))       # 'webapp' or 'scheduled'
    timestamp = Column(DateTime, default=datetime.utcnow)

# --- Table 2: Data Quality ---
class DataQuality(Base):
    __tablename__ = 'data_quality'
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255))
    total_rows = Column(Integer)
    valid_rows = Column(Integer)
    invalid_rows = Column(Integer)
    error_types = Column(Text)       # JSON string of error counts
    criticality = Column(String(20)) # 'low', 'medium', 'high'
    timestamp = Column(DateTime, default=datetime.utcnow)

# --- Database connection ---
# NOTE: @ symbol in password must be URL-encoded as %40
DATABASE_URL = "postgresql://job_user:JobUser%40123@localhost:5432/job_recommendation"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    Base.metadata.create_all(engine)
    print("✅ Database tables created successfully!")

if __name__ == "__main__":
    init_db()
