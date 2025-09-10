from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime
import pymysql
import os
from dotenv import load_dotenv

# Cargar .env
load_dotenv(dotenv_path="/home/ubuntu/app/.env")

api = FastAPI(title="API Cotización Dólar")

class RangoFechas(BaseModel):
    inicio: str
    fin: str

class Cotizacion(BaseModel):
    fechahora: str
    valor: float

class RespuestaCotizacion(BaseModel):
    datos: List[Cotizacion]

def conectar_mysql():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor
    )

@api.post("/consultar", response_model=RespuestaCotizacion)
def consultar_cotizaciones(rango: RangoFechas):
    try:
        f_inicio = datetime.strptime(rango.inicio, "%Y-%m-%d %H:%M:%S")
        f_fin = datetime.strptime(rango.fin, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato inválido: usar YYYY-MM-DD HH:MM:SS")

    try:
        con = conectar_mysql()
        cur = con.cursor()
        sql = """
            SELECT fechahora, valor
            FROM dolar
            WHERE fechahora BETWEEN %s AND %s
            ORDER BY fechahora ASC
        """
        cur.execute(sql, (f_inicio, f_fin))
        registros = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error DB: {str(e)}")
    finally:
        cur.close()
        con.close()

    return RespuestaCotizacion(datos=[Cotizacion(fechahora=str(r["fechahora"]), valor=r["valor"]) for r in registros])

@api.get("/ping")
def ping():
    return {"status": "ok"}
