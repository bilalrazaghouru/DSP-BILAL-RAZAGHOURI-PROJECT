import psycopg2
from psycopg2 import OperationalError

def test_connection():
    try:
        connection = psycopg2.connect(
            dbname="dsp_project1",
            user="dsp_user1",
            password="postgres123",   # your password
            host="localhost",
            port="5432"
        )
        print("✅ Connection successful!")
        connection.close()
    except OperationalError as e:
        print("❌ Connection failed!")
        print(e)

if __name__ == "__main__":
    test_connection()
