import json
from io import BytesIO
import pytest
from db_loader import g

# Cursor y conexión falsa
class FakeCursor:
    def executemany(self, q, vals):
        print(">>> SQL ejecutado:", q, "con", len(vals), "registros")
    def close(self):
        pass

class FakeConnection:
    def cursor(self):
        return FakeCursor()
    def commit(self):
        print(">>> Commit simulado")
    def close(self):
        print(">>> Cierre simulado")

# Contexto falso para Lambda
class DummyContext:
    db_conn = FakeConnection()

def test_db_loader(monkeypatch):
    event = {
        "Records": [{
            "s3": {"bucket": {"name": "fake-bucket"}, "object": {"key": "test.json"}}
        }]
    }

    # Mock boto3 para devolver JSON de prueba
    class FakeS3:
        def get_object(self, Bucket, Key):
            data = [[1757077268000, 3959], [1757077299000, 3960]]
            return {"Body": BytesIO(json.dumps(data).encode())}

    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    # Ejecutar Lambda con DummyContext (que contiene db_conn falsa)
    resp = g(event, DummyContext())
    assert resp["status"] == "ok"
