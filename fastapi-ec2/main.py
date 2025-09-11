from fastapi import FastAPI
from datetime import datetime
from typing import List
from app.db import get_data_between

app = FastAPI()

@app.get("/dolar")
async def get_dolar(start: datetime, end: datetime):
    data = get_data_between(start, end)
    return {"data": data}
