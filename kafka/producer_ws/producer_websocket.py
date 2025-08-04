import websocket
import json
import time
import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading

symbol = 'btcusdt'
timeframe_seconds = 5 * 60
topic_name = "Binance_ohlcv_5m"

kafka_producer = None
current_candle = None
lock = threading.Lock()

def wait_for_kafka(bootstrap_servers='kafka:9092', retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connexion à Kafka établie")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka non disponible... tentative {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("Kafka inaccessible après plusieurs tentatives")

def candle_start(timestamp_ms):
    return int(timestamp_ms // (timeframe_seconds * 1000) * (timeframe_seconds * 1000))

def send_current_candle():
    global current_candle
    with lock:
        if current_candle:
            kafka_producer.send(topic_name, value=current_candle)
            kafka_producer.flush()
            print(f"🚀 Bougie envoyée par timer : {datetime.datetime.fromtimestamp(current_candle['timestamp']/1000)}")
            current_candle = None

def timer_loop():
    while True:
        time.sleep(timeframe_seconds)
        send_current_candle()

def on_message(ws, message):
    global current_candle
    trade = json.loads(message)

    # Vérifier que c’est bien un trade
    if "p" not in trade or "q" not in trade or "T" not in trade:
        print("🔎 Message ignoré (pas un trade ou incomplet)")
        return

    price = float(trade["p"])
    quantity = float(trade["q"])
    timestamp = int(trade["T"])
    candle_start_val = candle_start(timestamp)

    with lock:
        if current_candle is None or candle_start_val != current_candle["timestamp"]:
            # Envoyer l’ancienne bougie si elle existe
            if current_candle:
                kafka_producer.send(topic_name, value=current_candle)
                kafka_producer.flush()
                print(f"🚀 Bougie envoyée : {datetime.datetime.fromtimestamp(current_candle['timestamp']/1000)}")

            # Démarrer nouvelle bougie
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
            # Mise à jour de la bougie en cours
            current_candle["high"] = max(current_candle["high"], price)
            current_candle["low"] = min(current_candle["low"], price)
            current_candle["close"] = price
            current_candle["volume"] += quantity
            print(f"📊 Bougie partielle — O:{current_candle['open']} C:{current_candle['close']} V:{current_candle['volume']}")

def on_error(ws, error):
    print("❌ Erreur WebSocket :", error)

def on_close(ws, close_status_code, close_msg):
    print("🔌 WebSocket fermé :", close_msg)


if __name__ == "__main__":
    kafka_producer = wait_for_kafka()

    # Lancer le timer dans un thread séparé
    timer_thread = threading.Thread(target=timer_loop, daemon=True)
    timer_thread.start()

    print("🚀 Démarrage du producer WebSocket...")
    websocket_url = f"wss://stream.binance.com:9443/ws/btcusdt@trade"
    ws = websocket.WebSocketApp(
        websocket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

        