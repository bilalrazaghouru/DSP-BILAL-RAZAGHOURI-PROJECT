from fastapi import FastAPI, Depends 
from sqlalchemy.orm import Session 
from pydantic import BaseModel 
from database import SessionLocal 
from models import Student 
 
app = FastAPI(title="Student API") 
 
def get_db(): 
    db = SessionLocal() 
    try: 
        yield db 
    finally: 
        db.close() 
 
class StudentCreate(BaseModel): 
    name: str 
    course: str 
 
@app.get("/") 
def root(): 
    return {"message": "Welcome to the Student API!"} 
 
@app.post("/add_student") 
def add_student(student: StudentCreate, db: Session = Depends(get_db)): 
    new_student = Student(name=student.name, course=student.course) 
    db.add(new_student) 
    db.commit() 
    db.refresh(new_student) 
    return {"id": new_student.id, "name": new_student.name, "course": new_student.course} 
 
@app.get("/get_students") 
def get_students(db: Session = Depends(get_db)): 
    students = db.query(Student).all() 
    return [{"id": s.id, "name": s.name, "course": s.course} for s in students]
