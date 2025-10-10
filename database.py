import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Only take from ENV; default is dsp:dsp on localhost
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://dsp:dsp@localhost:5432/dsp")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
