import os
import sys
import json
import boto3
import pymysql
import datetime

s3 = boto3.client("s3")

def g(event, context):
    # --- Validación temprana de variables de entorno ---
    required_env_vars = ["DB_HOST", "DB_USER", "DB_PASS", "DB_NAME"]
    missing = [var for var in required_env_vars if var not in os.environ]

    if missing:
        print(f"*** ERROR: Missing environment variables: {', '.join(missing)} ***")
        return {"status": "error", "missing": missing}
    # ---------------------------------------------------

    # Nombre del bucket y del archivo desde el evento
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    print(f">>> Procesando archivo {key} desde {bucket}")

    # Descargar el JSON de S3
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))

    # Conectar a la DB
    conn = pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"]
    )
    cursor = conn.cursor()

    # Insertar un registro por cada objeto del JSON
    for row in data:
        ts_ms = int(row[0])  # timestamp en milisegundos
        fechahora = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
        valor = float(row[1])

        cursor.execute(
            "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)",
            (fechahora, valor)
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(">>> Datos insertados correctamente en la DB")
    return {"status": "ok"}
