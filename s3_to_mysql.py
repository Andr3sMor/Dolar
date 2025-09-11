import os
import json
import boto3
from datetime import datetime
import pymysql
from botocore.config import Config

def g(event, context):
    """
    Lambda que carga datos de un archivo JSON en S3 a MySQL.
    Permite inyectar conexión para tests a través de context.db_conn.
    """
    db_conn = getattr(context, "db_conn", None)  # solo para tests
    try:
        print(">>> Iniciando Lambda db_loader...")

        # 1. Extraer info del evento S3
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        print(f">>> Procesando archivo {key} desde bucket {bucket}")

        # 2. Descargar archivo desde S3
        print("Debug")
        s3 = boto3.client("s3", config=Config(
                            connect_timeout=10,
                            read_timeout=30,
                            retries={'max_attempts': 3}
                        ))
        print("s3: ", s3)
        obj = s3.get_object(Bucket=bucket, Key=key)
        print("obj: ", obj)
        body = obj["Body"].read()
        print("body: ", body)
        print(f">>> Archivo descargado, {len(body)} bytes")

        # 3. Cargar JSON
        data = json.loads(body)
        print(f">>> JSON cargado correctamente: {type(data)}")

        # 4. Convertir datos
        rows = []
        for i, row in enumerate(data):
            timestamp_ms, valor = row
            fechahora = datetime.fromtimestamp(int(timestamp_ms) / 1000)
            valor = float(valor)
            rows.append((fechahora, valor))
            if i < 5:
                print(f"Fila {i}: {row} → ({fechahora}, {valor})")
        print(f">>> Total de filas preparadas: {len(rows)}")

        # 5. Conectar a la DB si no hay conexión inyectada
        cerrar_conexion = False
        if db_conn is None:
            print(">>> Conectando a DB real...")
            db_conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME"),
                cursorclass=pymysql.cursors.Cursor,
            )
            cerrar_conexion = True  # solo cerrar si es real

        # 6. Insertar en batch
        cursor = db_conn.cursor()
        insert_query = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
        chunk_size = 500
        for i in range(0, len(rows), chunk_size):
            batch = rows[i:i + chunk_size]
            cursor.executemany(insert_query, batch)
        db_conn.commit()
        cursor.close()

        if cerrar_conexion:
            db_conn.close()

        print(f">>> Inserción completada: {len(rows)} registros")
        return {"status": "ok", "rows": len(rows)}

    except Exception as e:
        print(">>> ERROR en Lambda:", e)
        return {"status": "error", "message": str(e)}
