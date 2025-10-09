from sqlalchemy import Column, Integer, String
from database import Base

# ✅ Define the "students" table structure
class Student(Base):
    __tablename__ = "students"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    course = Column(String, nullable=False)
