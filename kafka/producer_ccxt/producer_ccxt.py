import ccxt
import time
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Paramètres
symbol = 'BTC/USDT'
timeframe = '5m' 
limit = 500

# Exchange
exchange = ccxt.binance()

# Chargement de la date de départ
def load_last_timestamp(filename="last_timestamp.txt"):
    try:
        with open(filename, "r") as f:
            return int(f.read().strip())
    except FileNotFoundError:
        print("Aucun fichier de timestamp trouvé. Démarrage à partir de 2024-08-01")
        return exchange.parse8601("2024-08-01T00:00:00Z")

def save_last_timestamp(ts, filename="last_timestamp.txt"):
    with open(filename, "w") as f:
        f.write(str(ts))
        
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
since = load_last_timestamp()

print(f"Démarrage à partir de {since} pour {symbol} ({timeframe})")

try:
    while True:
        candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)

        if candles:
            for candle in candles:
                data = {
                    "timestamp": candle[0],      # ms
                    "open": candle[1],
                    "high": candle[2],
                    "low": candle[3],
                    "close": candle[4],
                    "volume": candle[5],
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "source" : "ccxt" ###### RAJOUT POUR CLASSER PAR SOURCE DE DONNEES
                }
                kafka_producer.send("Binance_ohlcv_5m", value=data)

            print(f"{len(candles)} bougies envoyées à Kafka depuis {since}")
            since = candles[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            save_last_timestamp(since)
            time.sleep(exchange.rateLimit / 1000)
        else:
            print("Aucune nouvelle bougie, attente...")
            time.sleep(5)

except KeyboardInterrupt:
    print("Arrêt manuel du producer")
