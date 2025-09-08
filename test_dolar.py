import json
import boto3
import pytest
import requests
import requests_mock
from moto import mock_aws
from dolar import obtener_y_guardar_dolar

@mock_aws
def test_obtener_y_guardar_dolar(monkeypatch):
    """
    Prueba unitaria para la función `obtener_y_guardar_dolar`.

    Escenario:
    - Se simula un bucket en S3 usando `moto` para evitar llamadas reales a AWS.
    - Se hace mock de `requests.get` para devolver datos simulados del servicio externo.
    - Se ejecuta la función que obtiene el valor del dólar y lo guarda en S3.
    
    Validación:
    - La función debe devolver un resultado con status "ok".
    - El archivo generado en S3 debe comenzar con el prefijo "dolar/dolar-".
    - El contenido guardado en S3 debe coincidir exactamente con el JSON de prueba.
    """
    
    # Crear bucket de prueba en el mock
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "dolar-raw-eltimpeosj"
    s3.create_bucket(Bucket=bucket_name)

    # Mock de requests.get para no llamar al servicio real
    sample_data = {"dolar": "5000"}
    with requests_mock.Mocker() as m:
        """
        Simula la respuesta del servicio externo del Banco de la República.
        - Devuelve un JSON con el valor del dólar.
        - Respuesta HTTP 200 para representar éxito.
        """
        m.get(
            "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario",
            json=sample_data,
            status_code=200
        )

        # Ejecutar función
        result = obtener_y_guardar_dolar()

        # Verificar resultado
        assert result["status"] == "ok"
        assert result["file"].startswith("dolar/dolar-")

        # Verificar que realmente subió a S3
        obj = s3.get_object(Bucket=bucket_name, Key=result["file"])
        contenido = json.loads(obj["Body"].read().decode("utf-8"))
        assert contenido == sample_data
