from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import mysql.connector

app = FastAPI()

def get_db_connection():
    conn = mysql.connector.connect(
        host='localhost',
        database='appweb',
        user='root',
        password='',
        port=3307
    )
    return conn

class Registro(BaseModel):
    nombre: str
    valor: float

@app.post("/api/insertar")
async def insertar(registro: Registro):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("INSERT INTO registros (nombre, valor) VALUES (%s, %s)", (registro.nombre, registro.valor))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    
    return JSONResponse(content={"message": "Registro insertado exitosamente."}, status_code=200)

@app.get("/api/consultar")
async def consultar():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM registros")
    registros = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return registros

@app.get("/api/estadisticas")
async def estadisticas():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT MAX(valor) as max, MIN(valor) as min, AVG(valor) as avg FROM registros")
    estadisticas = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    return {"max": estadisticas[0], "min": estadisticas[1], "avg": estadisticas[2]}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5001)
