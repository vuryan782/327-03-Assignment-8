import psycopg2

DATABASE_URL = //YOUR_CONNECTION_LINK
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
