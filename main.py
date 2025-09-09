from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime
import pymysql
import os
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv(dotenv_path="/home/ubuntu/app/.env")

app = FastAPI(title="API de valores del dólar")

class Consulta(BaseModel):
    fecha_inicio: str  # en formato "YYYY-MM-DD HH:MM:SS"
    fecha_fin: str

class ValorItem(BaseModel):
    fechahora: str
    valor: float

class Respuesta(BaseModel):
    datos: List[ValorItem]

def obtener_conexion():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor
    )

@app.post("/valores", response_model=Respuesta)
def consultar_valores(consulta: Consulta):
    try:
        fecha_inicio = datetime.strptime(consulta.fecha_inicio, "%Y-%m-%d %H:%M:%S")
        fecha_fin = datetime.strptime(consulta.fecha_fin, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido, usar YYYY-MM-DD HH:MM:SS")

    try:
        conexion = obtener_conexion()
        cursor = conexion.cursor()
        query = """
            SELECT fechahora, valor
            FROM dolar
            WHERE fechahora BETWEEN %s AND %s
            ORDER BY fechahora ASC
        """
        cursor.execute(query, (fecha_inicio, fecha_fin))
        resultados = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en DB: {str(e)}")
    finally:
        cursor.close()
        conexion.close()

    datos = [ValorItem(fechahora=str(r["fechahora"]), valor=r["valor"]) for r in resultados]
    return Respuesta(datos=datos)

@app.get("/big")
def hola():
    return {"Big Data"}