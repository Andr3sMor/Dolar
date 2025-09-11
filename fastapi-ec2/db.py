from dotenv import load_dotenv
import os
import pymysql

load_dotenv()  # Cargar variables desde .env

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}

def get_data_between(start, end):
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        SELECT fechahora, valor 
        FROM dólar
        WHERE fechahora BETWEEN %s AND %s
    """
    cursor.execute(query, (start, end))
    results = cursor.fetchall()
    conn.close()
    return [{"fechahora": str(row[0]), "valor": row[1]} for row in results]
