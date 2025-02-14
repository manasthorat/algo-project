import psycopg2

try:
    conn = psycopg2.connect(
        dbname="significant_weeks",
        user="postgres",
        password="Jetstar135@",
        host="localhost",
        port="5432"
    )
    print("Connected to PostgreSQL successfully!")
    conn.close()
except Exception as e:
    print("Error:", e)
