"""
Pruebas de error para db_loader.py
Estas pruebas validan que la Lambda maneja correctamente entradas inválidas
y excepciones al interactuar con S3 o la base de datos.
"""

import json
from io import BytesIO
import pytest
from db_loader import g


class FakeCursor:
    def executemany(self, q, vals):
        raise Exception("Error en la base de datos simulado")
    def close(self): pass


class FakeConnection:
    def cursor(self):
        return FakeCursor()
    def commit(self): pass
    def close(self): pass


def test_db_loader_json_invalido(monkeypatch):
    """
    Caso: El archivo en S3 contiene JSON corrupto → la función debe retornar error.
    """
    class FakeS3:
        def get_object(self, Bucket, Key):
            # JSON inválido
            return {"Body": BytesIO(b"{no valido json]")}
    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    event = {"Records": [{"s3": {"bucket": {"name": "fake"}, "object": {"key": "bad.json"}}}]}
    resp = g(event, None, FakeConnection())
    assert resp["status"] == "error"
    assert "JSON" in resp["message"] or "Expecting" in resp["message"]


def test_db_loader_event_malformado(monkeypatch):
    """
    Caso: Evento S3 no contiene Records → la función debe fallar con error.
    """
    event = {"Records": []}  # vacío
    resp = g(event, None, FakeConnection())
    assert resp["status"] == "error"


def test_db_loader_error_db(monkeypatch):
    """
    Caso: La DB lanza excepción en el insert → la función retorna error.
    """
    class FakeS3:
        def get_object(self, Bucket, Key):
            # JSON válido pero simple
            return {"Body": BytesIO(json.dumps([[1757077268000, 3959]]).encode())}
    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    event = {"Records": [{"s3": {"bucket": {"name": "fake"}, "object": {"key": "test.json"}}}]}
    resp = g(event, None, FakeConnection())
    assert resp["status"] == "error"
    assert "Error en la base de datos" in resp["message"]
