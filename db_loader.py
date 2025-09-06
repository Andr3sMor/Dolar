import os
import boto3
import pymysql
import datetime
import json
from io import BytesIO

def g(event, context):
    import polars as pl
    print("Instancing..")
    print(f"Zappa Event: {event}")

    # 1. Extraer info del S3 event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    print(f">>> Procesando archivo {key} desde {bucket}")

    # 2. Descargar archivo desde S3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    # 3. Intentar leer como objetos JSON primero
    try:
        df = pl.read_json(BytesIO(body))
        print("Formato detectado: JSON de objetos")
    except Exception:
        # Si falla, probar como array de arrays
        print("Formato detectado: JSON de arrays")
        raw = json.loads(body.decode("utf-8"))
        df = pl.DataFrame(raw, schema=["timestamp", "valor"])
        # convertir timestamp ms → datetime
        df = df.with_columns([
            (pl.col("timestamp").cast(pl.Int64) / 1000)
            .cast(pl.Datetime("ms"))
            .alias("fechahora"),
            pl.col("valor").cast(pl.Float64)
        ])
        df = df.select(["fechahora", "valor"])

    # 4. Si vienen columnas separadas de fecha+hora
    if "fecha" in df.columns and "hora" in df.columns:
        df = df.with_columns([
            pl.concat_str([pl.col("fecha"), pl.col("hora")], separator=" ").alias("fechahora")
        ])
    elif "timestamp" in df.columns and "fechahora" not in df.columns:
        df = df.with_columns([
            (pl.col("timestamp").cast(pl.Int64) / 1000)
            .cast(pl.Datetime("ms"))
            .alias("fechahora")
        ])

    # 5. Validaciones finales
    if "fechahora" not in df.columns:
        raise ValueError("El JSON debe contener 'fechahora' o 'timestamp' convertible a datetime")
    if "valor" not in df.columns:
        raise ValueError("El JSON debe contener la columna 'valor'")

    # 6. Conectar a MySQL
    connection = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.Cursor,
    )

    # 7. Insertar en batch
    with connection.cursor() as cursor:
        insert_query = """
            INSERT INTO dolar_data (fechahora, valor)
            VALUES (%s, %s)
        """
        data = [(fh, val) for fh, val in zip(df["fechahora"].to_list(), df["valor"].to_list())]

        chunk_size = 500
        for i in range(0, len(data), chunk_size):
            batch = data[i:i+chunk_size]
            cursor.executemany(insert_query, batch)

    connection.commit()
    connection.close()

    print(f"Inserción completada: {len(df)} registros")
    return {"status": "ok", "rows": len(df)}
