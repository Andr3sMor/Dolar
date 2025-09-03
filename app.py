from dolar import obtener_y_guardar_dolar
from datetime import datetime

def f(event, context):
    hoy = datetime.utcnow().date()
    limite = datetime(2025, 9, 9).date()  # <--- hasta el 9 de septiembre 2025

    if hoy > limite:
        print("El proceso ya no se ejecuta después del 9 de septiembre.")
        return {"status": "skipped", "message": "fuera de rango"}

    print("Lambda ejecutada")
    result = obtener_y_guardar_dolar()
    return result
