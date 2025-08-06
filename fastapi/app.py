from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd
import os
import psycopg2
from typing import List , Optional
from fastapi import Query

app = FastAPI(
    title = "Projet OPA"
)

POSTGRES_DB = os.environ.get("POSTGRES_DB", "data")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "data")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "data")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres") 
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")


@app.get("/predict")
def get_predict():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp, prediction, proba_achat FROM latest_prediction WHERE id = 1")
        result = cursor.fetchone()
        conn.close()
        if result:
            timestamp, prediction, proba = result
            return {
                "timestamp": timestamp,
                "prediction": prediction,
                "proba_achat": round(proba, 4) # La fonction round va arrondir proba à 4 chiffre
            }
        else:
            return {"message": "Aucune prédiction encore disponible"}
                        
    except Exception as e:
        return {"error": str(e)}      

@app.get("/last_price") 
def get_last_price():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT last_price FROM latest_prediction WHERE id = 1")
        result = cursor.fetchone()
        conn.close()
        if result:
            last_price = result[0]
            return {
                "Dernier prix": last_price
            }
        else:
            return {"message": "Aucun volume encore disponible"}
                        
    except Exception as e:
        return {"error": str(e)}

### 
class Prediction(BaseModel):
    timestamp: str
    prediction: float
    proba_achat: float
    last_price: float
    volume: float

@app.get("/predictions/by-date", response_model=List[Prediction])
def get_predictions_by_date(date: Optional[str] = Query(None, example="2025-08-04")):

    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        with conn.cursor() as cur:
            if date:
                cur.execute("""
                SELECT timestamp, prediction, proba_achat, last_price, volume
                FROM prediction_history
                WHERE DATE(timestamp) = %s
                ORDER BY timestamp DESC;
            """, (date,))

            else:
                cur.execute("""
                    SELECT timestamp, prediction, proba_achat, last_price, volume
                    FROM prediction_history
                    ORDER BY timestamp DESC
                    LIMIT 10;
                """)
            rows = cur.fetchall()
        conn.close()

        return [
            Prediction(
                timestamp=row[0].isoformat(),
                prediction=row[1],
                proba_achat=row[2],
                last_price=row[3],
                volume=row[4]
            )
            for row in rows
        ]

    except Exception as e:
        return {"error": str(e)}