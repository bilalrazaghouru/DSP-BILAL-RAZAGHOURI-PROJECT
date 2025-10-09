from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ✅ PostgreSQL connection URL
DATABASE_URL = "postgresql+psycopg2://dsp_user1:postgres123@localhost:5432/dsp_project1"

# ✅ Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# ✅ Create a SessionLocal class for DB sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ✅ Base class for model classes
Base = declarative_base()
