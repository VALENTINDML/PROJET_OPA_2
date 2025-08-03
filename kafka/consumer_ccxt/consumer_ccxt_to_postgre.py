import psycopg2
import json
import time
import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import os

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

# Connexion Kafka avec retry
def wait_for_kafka():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='binance-consumer-ohlcv-5m'
            )
            print("Connexion Kafka réussie.")
            return consumer
        except NoBrokersAvailable:
            print(f"Kafka pas encore prêt... tentative {i+1}/10")
            time.sleep(5)
    raise Exception("Impossible de se connecter à Kafka.")

# Connexions
conn = wait_for_postgres()
cur = conn.cursor()
consumer = wait_for_kafka()

# Création de la table pour les bougies
cur.execute("""
    CREATE TABLE IF NOT EXISTS binance_ohlcv_5m (
        timestamp TIMESTAMP PRIMARY KEY,
        symbol TEXT,
        timeframe TEXT,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        source TEXT
    )
""")
conn.commit()

print("Consumer OHLCV démarré et prêt à consommer.")

# Fonction d'insertion d’une bougie
def insert_candle(data):
    try:
        timestamp = datetime.datetime.fromtimestamp(int(data["timestamp"]) / 1000)
        cur.execute("""
            INSERT INTO binance_ohlcv_5m (timestamp, symbol, timeframe, open, high, low, close, volume, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING
        """, (
            timestamp,
            data["symbol"],
            data["timeframe"],
            float(data["open"]),
            float(data["high"]),
            float(data["low"]),
            float(data["close"]),
            float(data["volume"]),
            data.get("source" , "unknown") ######### RAJOUT POUR CLASSER PAR SOURCE DE DONNEE
        ))
        conn.commit()
        print(f"Bougie insérée à {timestamp}")
    except Exception as e:
        print(f"Erreur insertion : {e}")

# Boucle de consommation
for message in consumer:
    insert_candle(message.value)
