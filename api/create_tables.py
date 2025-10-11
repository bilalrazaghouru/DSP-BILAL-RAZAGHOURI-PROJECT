from database import Base, engine
import models  # ensure SQLAlchemy models are registered

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created.")
