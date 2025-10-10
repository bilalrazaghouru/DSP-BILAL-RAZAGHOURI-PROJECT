import sys, os
sys.path.append(os.path.dirname(__file__))

from database import SessionLocal
from models import Student

# ✅ Create a new database session
db = SessionLocal()

# ✅ Create a new student record
new_student = Student(name="Bilal RazaghouRi", course="Data Science")

# ✅ Add it to the session and commit
db.add(new_student)
db.commit()
db.refresh(new_student)

print(f"✅ Added Student: {new_student.id} - {new_student.name} ({new_student.course})")

# ✅ Fetch all students from DB
all_students = db.query(Student).all()
print("\n📋 All Students:")
for s in all_students:
    print(f"{s.id}: {s.name} - {s.course}")

db.close()
