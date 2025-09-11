from fastapi import FastAPI, HTTPException, Form
from typing import List
from pydantic import BaseModel
from datetime import datetime
import pymysql
import os
from dotenv import load_dotenv

# Cargar las variables de entorno
load_dotenv()

app = FastAPI()

# Definir el modelo de la consulta
class DateRange(BaseModel):
    start: datetime
    end: datetime

class ValorItem(BaseModel):
    fechahora: str
    valor: float

class Respuesta(BaseModel):
    datos: List[ValorItem]

# Función para obtener la conexión a la base de datos
def obtener_conexion():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor
    )

@app.post("/dolar", response_model=Respuesta)
async def get_dolar(date_range: DateRange):
    start = date_range.start
    end = date_range.end

    try:
        conexion = obtener_conexion()
        cursor = conexion.cursor()
        query = """
            SELECT fechahora, valor
            FROM dolar
            WHERE fechahora BETWEEN %s AND %s
            ORDER BY fechahora ASC
        """
        cursor.execute(query, (start, end))
        results = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en DB: {str(e)}")
    finally:
        cursor.close()
        conexion.close()

    # Formatear la respuesta
    data = [ValorItem(fechahora=str(r["fechahora"]), valor=r["valor"]) for r in results]
    return Respuesta(datos=data)
