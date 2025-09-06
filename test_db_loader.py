# tests/test_db_loader.py
import json
import pytest
import pymysql
import boto3
from moto import mock_aws
from db_loader import g

BUCKET = "dolar-raw-eltimpeosj"

@mock_aws
def test_db_loader(monkeypatch):
    # Crear bucket simulado
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    # Subir archivo de prueba
    data = [
        ["1756818048000", "4040"],
        ["1756818052000", "4040"]
    ]
    s3.put_object(Bucket=BUCKET, Key="test.json", Body=json.dumps(data))

    # Mockear conexión a DB
    class FakeCursor:
        def execute(self, q, vals=None):
            print("SQL execute:", q, vals)
    
        def executemany(self, q, seq_of_vals):
            for vals in seq_of_vals:
                self.execute(q, vals)
    
        def fetchone(self):
            return ("2025-09-04 12:00:00",)  # valor dummy para el test
    
        def close(self):
            pass
    
        def __enter__(self):
            return self
    
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    
    class FakeConn:
        def cursor(self): 
            return FakeCursor()
    
        def commit(self): 
            print("COMMIT")
    
        def close(self): 
            print("CLOSE")



    monkeypatch.setattr(pymysql, "connect", lambda **kwargs: FakeConn())

    # Mockear variables de entorno requeridas por db_loader
    monkeypatch.setenv("DB_HOST", "fake-host")
    monkeypatch.setenv("DB_USER", "fake-user")
    monkeypatch.setenv("DB_PASS", "fake-pass")
    monkeypatch.setenv("DB_NAME", "fake-db")

    # Evento simulado de S3
    event = {
        "Records": [{
            "s3": {
                "bucket": {"name": BUCKET},
                "object": {"key": "test.json"}
            }
        }]
    }

    # Llamar Lambda
    resp = g(event, None)
    assert resp["status"] == "ok"
