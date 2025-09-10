from dolar_handler import guardar_cotizacion
from datetime import datetime

def ejecutar_lambda(event, context):
    """
    Lambda principal que obtiene y guarda la cotización del dólar en S3.
    """
    print(">>> Lambda disparada para obtener cotización del dólar")
    resultado = guardar_cotizacion()
    return resultado
