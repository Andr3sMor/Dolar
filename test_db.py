import json
from io import BytesIO
import pytest
from db_loader import g

# Cursor y conexión falsa
class FakeCursor:
    """
    Cursor simulado para pruebas unitarias.
    - Implementa 'executemany' para registrar consultas SQL simuladas.
    - Implementa 'close' sin acción real.
    """
    def executemany(self, q, vals):
        print(">>> SQL ejecutado:", q, "con", len(vals), "registros")
    def close(self):
        pass

class FakeConnection:
    """
    Conexión simulada a la base de datos para pruebas unitarias.
    - Devuelve un FakeCursor en lugar de un cursor real.
    - Implementa 'commit' y 'close' para simular operaciones de base de datos.
    """
    def cursor(self):
        return FakeCursor()
    def commit(self):
        print(">>> Commit simulado")
    def close(self):
        print(">>> Cierre simulado")

# Contexto falso para Lambda
class DummyContext:
    """Un objeto de contexto de AWS Lambda ficticio para pruebas, que contiene una conexión de base de datos falsa."""
    db_conn = FakeConnection()

def test_db_loader(monkeypatch):
    """
    Prueba unitaria para la función Lambda `db_loader.g`.

    Escenario:
    - Se simula un evento S3 con un archivo JSON.
    - Se hace mock del cliente boto3 para devolver datos de prueba.
    - Se usa FakeConnection para evitar conexión real a MySQL.
    
    Validación:
    - Se espera que la función procese correctamente el archivo JSON.
    - Se verifica que el resultado devuelto tenga status 'ok'.
    """
    event = {
        "Records": [{
            "s3": {"bucket": {"name": "fake-bucket"}, "object": {"key": "test.json"}}
        }]
    }

    # Mock boto3 para devolver JSON de prueba
    class FakeS3:
        """
        Cliente S3 simulado.
        - Devuelve un objeto JSON con dos filas de datos de ejemplo.
        """
        def get_object(self, Bucket, Key):
            data = [[1757077268000, 3959], [1757077299000, 3960]]
            return {"Body": BytesIO(json.dumps(data).encode())}

    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    # Ejecutar Lambda con DummyContext (que contiene db_conn falsa)
    resp = g(event, DummyContext())
    assert resp["status"] == "ok"
