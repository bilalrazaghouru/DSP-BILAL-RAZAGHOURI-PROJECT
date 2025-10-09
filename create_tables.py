import sys, os
sys.path.append(os.path.dirname(__file__))

from database import Base, engine
from models import Student

print("🚀 Creating all tables in database...")
Base.metadata.create_all(bind=engine)
print("✅ Tables created successfully!")
