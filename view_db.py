import sqlite3

conn = sqlite3.connect("db/app.db")
c = conn.cursor()

rows = c.execute("SELECT * FROM predictions;").fetchall()

print("📊 Predictions stored in DB:")
for row in rows:
    print(row)

conn.close()
