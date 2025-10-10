# create_tables.py
from database import Base, engine
import models  # ensures Prediction is registered

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created.")
