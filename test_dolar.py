import json
import boto3
import pytest
import requests
import requests_mock
from moto import mock_aws
from dolar import obtener_y_guardar_dolar

@mock_aws
def test_obtener_y_guardar_dolar(monkeypatch):
    # Crear bucket de prueba en el mock
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "dolar-raw-eltimpeosj"
    s3.create_bucket(Bucket=bucket_name)

    # Mock de requests.get para no llamar al servicio real
    sample_data = {"dolar": "5000"}
    with requests_mock.Mocker() as m:
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
