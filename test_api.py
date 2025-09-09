"""
Pruebas para la API FastAPI que expone el endpoint de consulta de valores.
Se valida tanto el flujo exitoso como casos de error y entradas inválidas.
"""

from fastapi.testclient import TestClient
import pytest
from main import app

client = TestClient(app)


def test_consulta_valores_ok(monkeypatch):
    """
    Caso feliz: El cliente envía un rango válido de fechas
    y la base de datos devuelve registros.
    """

    class FakeCursor:
        def execute(self, q, params): pass
        def fetchall(self):
            return [
                {"fechahora": "2025-09-01 07:00:00", "valor": 3950.0},
                {"fechahora": "2025-09-01 08:00:00", "valor": 3960.0},
            ]
        def close(self): pass

    class FakeConn:
        def cursor(self, cursorclass=None): return FakeCursor()
        def close(self): pass

    # Parchar conexión
    monkeypatch.setattr("main.obtener_conexion", lambda: FakeConn())

    body = {"fecha_inicio": "2025-09-01 07:00:00", "fecha_fin": "2025-09-01 09:00:00"}
    resp = client.post("/valores", json=body)

    assert resp.status_code == 200
    datos = resp.json()["datos"]
    assert len(datos) == 2
    assert datos[0]["valor"] == 3950.0


def test_consulta_valores_sin_datos(monkeypatch):
    """
    Caso: El rango de fechas no devuelve resultados.
    """
    class FakeCursor:
        def execute(self, q, params): pass
        def fetchall(self): return []
        def close(self): pass

    class FakeConn:
        def cursor(self, cursorclass=None): return FakeCursor()
        def close(self): pass

    monkeypatch.setattr("main.obtener_conexion", lambda: FakeConn())

    body = {"fecha_inicio": "2025-09-01 07:00:00", "fecha_fin": "2025-09-01 09:00:00"}
    resp = client.post("/valores", json=body)

    assert resp.status_code == 200
    assert resp.json()["datos"] == []


def test_consulta_valores_fechas_invalidas():
    """
    Caso: El cliente envía fechas en formato inválido.
    """
    body = {"fecha_inicio": "fecha_mala", "fecha_fin": "2025-09-01 09:00:00"}
    resp = client.post("/valores", json=body)
    assert resp.status_code == 400
    assert "Formato de fecha inválido" in resp.json()["detail"]


def test_consulta_valores_faltan_parametros():
    """
    Caso: El cliente no envía uno de los parámetros requeridos.
    """
    body = {"fecha_inicio": "2025-09-01 07:00:00"}  # falta fecha_fin
    resp = client.post("/valores", json=body)
    assert resp.status_code == 422  # error de validación de FastAPI
