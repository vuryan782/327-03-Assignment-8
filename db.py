import psycopg2
import os
from dotenv import load_dotenv

load_dotenv(".env.local") 

DATABASE_URL = os.getenv("DATABASE_URL")

def connect_db():
    return psycopg2.connect(DATABASE_URL)

def test_connection():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT NOW();")
    result = cur.fetchone()
    print("Connected. DB time:", result[0])
    cur.close()
    conn.close()

if __name__ == "__main__":
    test_connection()
