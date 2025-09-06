import json
from io import BytesIO
import pytest
from db_loader import g

class FakeCursor:
    def executemany(self, q, vals):
        print(">>> SQL ejecutado:", q, "con", len(vals), "registros")
    def close(self): pass

class FakeConnection:
    def cursor(self):
        return FakeCursor()
    def commit(self): print(">>> Commit simulado")
    def close(self): print(">>> Cierre simulado")

def test_db_loader(monkeypatch):
    event = {
        "Records": [{
            "s3": {"bucket": {"name": "fake-bucket"}, "object": {"key": "test.json"}}
        }]
    }

    # Mock boto3 para devolver JSON de prueba
    class FakeS3:
        def get_object(self, Bucket, Key):
            return {"Body": BytesIO(json.dumps([[1757077268000, 3959], [1757077299000, 3960]]).encode())}
    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    # Llamada a la Lambda usando conexión falsa
    resp = g(event, None, db_conn=FakeConnection())
    assert resp["status"] == "ok"
