"""
Pruebas de error para db_loader.py
Estas pruebas validan que la Lambda maneja correctamente entradas inválidas
y excepciones al interactuar con S3 o la base de datos.
"""
import json
from io import BytesIO
import types
import pytest
import db_loader

class FakeCursor:
    def __init__(self, fail=False):
        self.fail = fail
        self.executed = []
    def executemany(self, query, data):
        if self.fail:
            raise Exception("DB insert failed")
        self.executed.extend(data)
    def close(self): pass

class FakeConnection:
    def __init__(self, fail=False):
        self.fail = fail
        self.cursor_obj = FakeCursor(fail=fail)
    def cursor(self):
        return self.cursor_obj
    def commit(self): pass
    def close(self): pass

def make_context_with_db(fail=False):
    """Devuelve un contexto con db_conn inyectado (fake)."""
    ctx = types.SimpleNamespace()
    ctx.db_conn = FakeConnection(fail=fail)
    return ctx

def test_db_loader_json_invalido(monkeypatch):
    """Caso: El archivo en S3 contiene JSON corrupto → error"""
    class FakeS3:
        def get_object(self, Bucket, Key):
            return {"Body": BytesIO(b"{no valido json]")}
    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    event = {"Records": [{"s3": {"bucket": {"name": "fake"}, "object": {"key": "bad.json"}}}]}
    resp = db_loader.g(event, make_context_with_db())
    assert resp["status"] == "error"

def test_db_loader_event_malformado(monkeypatch):
    """Caso: Evento vacío → error"""
    monkeypatch.setattr("boto3.client", lambda service: None)
    event = {"Records": []}
    resp = db_loader.g(event, make_context_with_db())
    assert resp["status"] == "error"

def test_db_loader_error_db(monkeypatch):
    """Caso: La DB lanza excepción → error"""
    class FakeS3:
        def get_object(self, Bucket, Key):
            return {"Body": BytesIO(json.dumps([[1757077268000, 3959]]).encode())}
    monkeypatch.setattr("boto3.client", lambda service: FakeS3())

    event = {"Records": [{"s3": {"bucket": {"name": "fake"}, "object": {"key": "test.json"}}}]}
    resp = db_loader.g(event, make_context_with_db(fail=True))
    assert resp["status"] == "error"
