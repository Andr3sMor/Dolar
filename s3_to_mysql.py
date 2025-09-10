import os
import json
import boto3
from datetime import datetime
import pymysql

def cargar_datos(event, context):
    """
    Lambda que recibe un evento de S3 y carga datos a MySQL.
    """
    conexion_db = getattr(context, "db_conn", None) 
    try:
        print(">>> Iniciando Lambda carga S3 → MySQL...")

        # 1. Extraer evento de S3
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        print(f">>> Archivo recibido: {key} desde bucket {bucket}")

        # 2. Descargar objeto de S3
        s3_client = boto3.client("s3")
        objeto = s3_client.get_object(Bucket=bucket, Key=key)
        contenido = objeto["Body"].read()
        print(f">>> Descargados {len(contenido)} bytes")

        # 3. Parsear JSON
        registros = json.loads(contenido)
        filas = []
        for i, (ts, valor) in enumerate(registros):
            fecha = datetime.fromtimestamp(int(ts) / 1000)
            filas.append((fecha, float(valor)))
            if i < 3:
                print(f"Ejemplo fila {i}: {ts} → {fecha}, {valor}")

        print(f">>> Total filas preparadas: {len(filas)}")

        # 4. Conexión DB real si no se inyecta
        cerrar = False
        if conexion_db is None:
            conexion_db = pymysql.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME"),
                cursorclass=pymysql.cursors.Cursor,
            )
            cerrar = True

        # 5. Insertar en batch
        cursor = conexion_db.cursor()
        insert_sql = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
        batch_size = 500
        for i in range(0, len(filas), batch_size):
            cursor.executemany(insert_sql, filas[i:i+batch_size])
        conexion_db.commit()
        cursor.close()

        if cerrar:
            conexion_db.close()

        print(">>> Inserción finalizada correctamente")
        return {"status": "ok", "rows": len(filas)}

    except Exception as e:
        print(">>> ERROR durante la carga:", e)
        return {"status": "error", "message": str(e)}
