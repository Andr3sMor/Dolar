import requests
import json
from datetime import datetime
import boto3

BUCKET_NAME = "dolar-raw-awsmm"   

def guardar_cotizacion():
    """
    Descarga datos de cotización del Banco de la República
    y los guarda como JSON en S3.
    """
    s3 = boto3.client("s3")  
    url = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
    try:
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        datos = respuesta.json()

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nombre_archivo = f"dolar/dolar-{ts}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=nombre_archivo,
            Body=json.dumps(datos, indent=4),
            ContentType="application/json"
        )

        print(f"Archivo guardado en s3://{BUCKET_NAME}/{nombre_archivo}")
        return {"status": "ok", "file": nombre_archivo}

    except Exception as e:
        print(">>> Error guardando en S3:", e)
        return {"status": "error", "message": str(e)}