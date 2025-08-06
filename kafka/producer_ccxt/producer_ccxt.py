import ccxt
import time
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import datetime
import psycopg2
import os


# Paramètres
symbol = 'BTC/USDT'
timeframe = '5m' 
limit = 288

# Exchange
exchange = ccxt.binance()

def get_last_timestamp_from_db():
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("POSTGRES_DB", "data"),
            user=os.environ.get("POSTGRES_USER", "data"),
            password=os.environ.get("POSTGRES_PASSWORD", "data"),
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=os.environ.get("POSTGRES_PORT", "5432")
        )
        cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM ccxt_ohlcv;")
        result = cur.fetchone()
        conn.close()

        if result and result[0]:
            print(f"Dernier timestamp trouvé en base : {result[0]}")
            return int(result[0].timestamp() * 1000)  # convertit en ms
        else:
            start_date = datetime.datetime(2025, 8, 3, 0, 0)
            print(f"Table vide. Démarrage forcé au {start_date} UTC")
            return int(start_date.timestamp() * 1000)

    except Exception as e:
        print(f"Erreur connexion PostgreSQL : {e}")
        # En cas d'erreur, on choisit aussi le 5 août 2025
        start_date = datetime.datetime(2025, 8, 3, 0, 0)
        print(f"Erreur DB. Démarrage forcé au {start_date} UTC")
        return int(start_date.timestamp() * 1000)

        
# Attente de Kafka
def wait_for_kafka(bootstrap_servers='kafka:9092', retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connexion à Kafka établie")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka non disponible... tentative {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("Kafka inaccessible après plusieurs tentatives")

# Kafka setup
kafka_producer = wait_for_kafka()

# Point de départ
since = get_last_timestamp_from_db()

print(f"Démarrage à partir de {since} pour {symbol} ({timeframe})")

candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)

if candles:
    if candles[0][0] == since: 
        print(f"Suppression de la première bougie à {since} qui à déjà était envoyé")
        candles= candles[1:]
    for candle in candles:
        data = {
            "timestamp": candle[0],      
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4],
            "volume": candle[5],
            "symbol": symbol,
            "timeframe": timeframe,
            "source" : "ccxt" ###### RAJOUT POUR CLASSER PAR SOURCE DE DONNEES
        }
        kafka_producer.send("btc_usdt", value=data)

    kafka_producer.flush()

    
    # Envoie de message au topic
    kafka_producer.send("btc_usdt" , value={
        "type":"Information",
        "timestamp": int(time.time() * 1000),
        "details": "Batch effectuer avec succes",
        "symbol": symbol

    })

    kafka_producer.flush()

else: 
    kafka_producer.send("btc_usdt", value={
        "type":"WARNING",
        "timestamp": int(time.time() * 1000),
        "details": "erreur du processus",
        "symbol": symbol
    })

    kafka_producer.flush()




