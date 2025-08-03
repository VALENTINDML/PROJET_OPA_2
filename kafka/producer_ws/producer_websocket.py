import websocket 
import json 
import time 
import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

symbol = 'btcusdt'
timeframe_seconds = 5 * 60 
topic_name = "Binance_ohlcv_5m"

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

kafka_producer = wait_for_kafka()

# Agrégation OHLCV 
current_candle = None 

def candle_start(timestamp_ms):
    return int(timestamp_ms // (timeframe_seconds * 1000) * (timeframe_seconds * 1000))

def on_message(ws, message):
    global current_candle

    trade = json.loads(message)
    price = float(trade["p"])
    quantity = float(trade["q"])
    timestamp = int(trade["T"])
    candle_start_val = candle_start(timestamp)

    # Si nouvelle bougie
    if current_candle is None or candle_start_val != current_candle["timestamp"]:
        # Envoyer la précédente bougie si elle existe
        if current_candle:
            kafka_producer.send(topic_name, value=current_candle)
            print(f"Bougie websocket envoyée : {datetime.datetime.fromtimestamp(current_candle['timestamp']/1000)}")

        # Initialiser nouvelle bougie
        current_candle = {
            "timestamp": candle_start_val,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": quantity,
            "symbol": "BTC/USDT",
            "timeframe": "5m",
            "source": "WebSocket"
        }
    else:
        # Mettre à jour la bougie existante
        current_candle["high"] = max(current_candle["high"], price)
        current_candle["low"] = min(current_candle["low"], price)
        current_candle["close"] = price
        current_candle["volume"] += quantity

def on_error(ws, error):
    print("Erreur WebSocket :", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket fermé :", close_msg)

def on_open(ws):
    print("Connexion WebSocket ouverte.")
    # S'abonner au flux de trades BTC/USDT
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@trade"],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

# Connexion au WebSocket Binance
websocket_url = f"wss://stream.binance.com:9443/ws" # Url simple car on utilise subscribe

ws = websocket.WebSocketApp(
    websocket_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

print("Démarrage du WebSocket Binance (WebSocket producer)...")

try:
    ws.run_forever()
except KeyboardInterrupt:
    print("Arrêt manuel du WebSocket producer")

        