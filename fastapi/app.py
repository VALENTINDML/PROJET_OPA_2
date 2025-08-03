from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd
import os

app = FastAPI()

POSTGRES_DB = os.environ.get("POSTGRES_DB", "binance_data")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
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


