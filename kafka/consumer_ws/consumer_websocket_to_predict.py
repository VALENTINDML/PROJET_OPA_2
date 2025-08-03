from kafka import KafkaConsumer
import json
import pickle
import numpy as np
import threading
import psycopg2
import os
import time

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "Binance_ohlcv_5m") 

# Chargement des variables d’environnement
POSTGRES_DB = os.environ.get("POSTGRES_DB", "binance_data")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres") 
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "Binance_ohlcv_5m") 

# Connexion PostgreSQL avec retry
def wait_for_postgres():
    for i in range(10):
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT
            )
            print("Connexion PostgreSQL établie.")
            return conn
        except psycopg2.OperationalError:
            print(f"PostgreSQL pas encore prêt... tentative {i+1}/10")
            time.sleep(5)
    raise Exception("Impossible de se connecter à PostgreSQL.")

def create_table(): 
    conn = wait_for_postgres()
    with conn.cursor() as cur :
        cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_history (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                prediction INTEGER,
                proba_achat FLOAT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS latest_prediction (
                id INTEGER PRIMARY KEY,
                timestamp TIMESTAMP,
                prediction INTEGER,
                proba_achat FLOAT
            )
        """)
    conn.commit()
    conn.close()

# Sauvegarde des prédictions
def save_prediction(timestamp, prediction, proba):
    conn = wait_for_postgres()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO prediction_history (timestamp, prediction, proba_achat)
            VALUES (%s, %s, %s)
        """, (timestamp, prediction, proba))

        cur.execute("""
            INSERT INTO latest_prediction (id, timestamp, prediction, proba_achat)
            VALUES (1, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                prediction = EXCLUDED.prediction,
                proba_achat = EXCLUDED.proba_achat
        """, (timestamp, prediction, proba))
    conn.commit()
    conn.close()
    
create_table() # Création des tables


# Charger le modèle
#with open("/app/models/model.pkl", "rb") as f:
    #model = pickle.load(f)

model_path = "/app/models/model.pkl"
while not os.path.exists(model_path):
    print(f"Le modèle {model_path} n'est pas encore disponible, attente 5s...")
    time.sleep(5)

with open(model_path, "rb") as f:
    model = pickle.load(f)
print("Modèle chargé avec succès.")

latest_prediction = {"prediction": None}

# Connexion Kafka avec retry
def wait_for_kafka():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='realtime-predictor'
            )
            print("Connexion Kafka réussie.")
            return consumer
        except NoBrokersAvailable:
            print(f"Kafka pas encore prêt... tentative {i+1}/10")
            time.sleep(5)
    raise Exception("Impossible de se connecter à Kafka.")

def listen_and_predict():
    consumer = wait_for_kafka()

    print("⏳ En attente de bougies WebSocket...")

    for message in consumer:
        data = message.value

        if data.get("source") != "WebSocket":
            continue  # Ignore les bougies CCXT

        features = np.array([[
            float(data["open"]),
            float(data["high"]),
            float(data["low"]),
            float(data["close"]),
            float(data["volume"])
        ]])

        prediction = model.predict(features)[0]
        proba = model.predict_proba(features)[0][1]

        latest_prediction["prediction"] = int(prediction) # Peut servir de faire l'endpoint /predict de l'api

        timestamp = data.get("timestamp")
        save_prediction(timestamp , int(prediction) , float(proba))
        print(f"🧠 Prédiction temps réel : {prediction} Proba temps réel : {proba}" )



