# Proyecto: Zappa Dólar

Este proyecto implementa un sistema serverless en **AWS Lambda** con **Zappa**, que:  
1. Obtiene automáticamente el valor del dólar desde el Banco de la República.  
2. Guarda los datos en un bucket S3.  
3. Procesa y carga los datos en una base de datos MySQL.  
4. Expone un endpoint con **FastAPI** para consultar intervalos de fechas.  


##  Estructura principal

- `dolar.py` → Lógica para obtener el valor del dólar y guardarlo en S3.  
- `db_loader.py` → Lambda que procesa archivos JSON desde S3 y los inserta en MySQL.  
- `app.py` → Endpoint con FastAPI para consultar datos por intervalo de fechas.  
- `zappa_settings.json` → Configuración de despliegues en AWS.  
- `test_dolar.py` → Pruebas unitarias para la función de carga a S3.  
- `test_db_loader.py` → Pruebas unitarias para la función de inserción en DB.  

## Dependencias principales

- **Zappa** → Despliegue en AWS Lambda.  
- **boto3** → Cliente para interactuar con AWS.  
- **pymysql** → Conector MySQL.  
- **pytest** → Framework de pruebas.  
- **requests** → Llamadas HTTP a APIs externas.  
- **moto** → Mock de AWS para pruebas locales.  
- **requests-mock** → Mock de requests para pruebas offline.  

---

## Pruebas

### Test de `dolar.py`

Archivo: `test_dolar.py`

```python
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
````

Este test garantiza la logica,**no se hacen llamadas reales** a AWS ni al Banco de la República.

---

### Test de `db_loader.py`

Archivo: `test_db_loader.py`

* Mockea **boto3** para devolver datos de prueba desde S3.
* Inyecta una **conexión simulada** (`FakeConnection`) para evitar conexión real a MySQL.
* Verifica que los datos se transformen e inserten correctamente.

---

## Cómo correr las pruebas

1. Instala las dependencias:

```bash
pip install -r requirements.txt
```

2. Ejecuta los tests con **pytest**:

```bash
pytest -v
```

3. Si quieres ver logs detallados en consola:

```bash
pytest -s -v
```

---

## Cron jobs

El proyecto usa **CloudWatch Events** para disparar funciones Lambda en horarios específicos.
Ejemplo:

* `"cron(0 23 ? * * *)"` → Ejecuta a las **6:00 p.m. hora Colombia (UTC-5)**.

---

## API (FastAPI)

Se incluye un endpoint para consultar datos en un intervalo de fecha/hora vía **POST**.

Ejemplo request:

```json
{
  "inicio": "2025-09-01T00:00:00",
  "fin": "2025-09-02T23:59:59"
}
```

Ejemplo response:

```json
{
  "status": "ok",
  "rows": [
    {"fechahora": "2025-09-01T10:00:00", "valor": 3959.0},
    {"fechahora": "2025-09-01T12:00:00", "valor": 3960.3}
  ]
}
```

---

##  Flujo del sistema

<img width="1661" height="81" alt="Diagrama sin título" src="https://github.com/user-attachments/assets/9cea2a2f-b6ec-42d5-ac6d-218c28b31967" />



