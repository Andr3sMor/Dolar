from fastapi import FastAPI
from datetime import datetime
from pydantic import BaseModel
from typing import List
from db import get_data_between

# Crear el modelo de datos para el cuerpo de la solicitud
class DateRange(BaseModel):
    start: datetime
    end: datetime

app = FastAPI()

# Endpoint para recibir el rango de fechas con POST
@app.post("/dolar")
async def get_dolar(date_range: DateRange):
    start = date_range.start
    end = date_range.end
    data = get_data_between(start, end)
    return {"data": data}
