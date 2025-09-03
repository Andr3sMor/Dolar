import requests
import json
from datetime import datetime
import boto3
import os

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "dolar-raw-eltimpeosj")

def obtener_y_guardar_dolar():
    """
    Obtiene los datos del mercado cambiario de la URL y los guarda en un JSON en S3.
    """
    url = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Crear nombre con timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"dolar/dolar-{timestamp}.json"
        
        # Subir a S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data, indent=4),
            ContentType="application/json"
        )
        
        print(f"Archivo guardado en s3://{BUCKET_NAME}/{filename}")
        return {"status": "ok", "file": filename}
    
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}
