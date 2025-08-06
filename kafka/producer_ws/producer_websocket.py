import websocket
import json
import time
import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading

symbol = 'btcusdt'  # Paire à extraire (En minuscule car c'est le format de Websocket)
timeframe_seconds = 5 * 60  # 5 minutes
topic_name = "btc_usdt"  # Nom du topic kafka 

kafka_producer = None
last_sent_candle_timestamp = None # timestamp de la dernière bougies envoyer 
current_candle = None # Stocke la bougie faites en temps réel 
lock = threading.Lock() # Gere les conflits sur current_candle

# Connexion Kafka
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



def candle_start(timestamp_ms):
    return int(timestamp_ms // (timeframe_seconds * 1000) * (timeframe_seconds * 1000))

"""Calcule le timestamp du début de la bougie 5 minutes correspondante
à un timestamp donné en millisecondes.
Par exemple, si timestamp_ms est à 12:08, cela retourne le timestamp de 12:05.
Formule pour regroupé les bougies dans le meme intervalle de 5min"""



def send_current_candle():
    global current_candle, last_sent_candle_timestamp
    with lock:
        if current_candle and current_candle["timestamp"] != last_sent_candle_timestamp:
            kafka_producer.send(topic_name, value=current_candle) # On envoie au topic la bougie 
            kafka_producer.flush() # Force l'envoie
            print(f"Bougie envoyée par timer : {datetime.datetime.fromtimestamp(current_candle['timestamp']/1000)}")
            last_sent_candle_timestamp = current_candle["timestamp"] # On met à jour la dernière bougie avec la bougie qui vient d'etre envoyé
            current_candle = None # On remet à 0 pour recommencer 

"""Envoie si current_candle n'est pas vide et que le timestamp et différent du dernier timestamp.
Remet current_candle à None après envoi pour re-créer une bougies 
Protégé par un verrou pour éviter les conflits."""



def timer_loop():
    while True:
        time.sleep(timeframe_seconds)
        send_current_candle()

"""Boucle infinie qui attend 5 minutes (timeframe_seconds),
puis envoie la bougie courante via send_current_candle()."""


def on_message(ws, message):
    global current_candle
    trade = json.loads(message)

    # Vérifier que c'est bien un trade
    if "p" not in trade or "q" not in trade or "T" not in trade:
        print("Message ignoré (pas un trade ou incomplet)")
        return

    # Les trades n'ont pas automatiquement les bons attributs donc on convertit
    price = float(trade["p"])
    quantity = float(trade["q"])
    timestamp = int(trade["T"])
    candle_start_val = candle_start(timestamp)

    with lock:
        # Nouvelle bougie si aucune en cours ou timestamp différent
        if current_candle is None or candle_start_val != current_candle["timestamp"]:
            # Envoyer l'ancienne bougie si elle existe
            if current_candle and current_candle["timestamp"] != last_sent_candle_timestamp:
                kafka_producer.send(topic_name, value=current_candle)
                kafka_producer.flush()
                print(f"Bougie envoyée : {datetime.datetime.fromtimestamp(current_candle['timestamp']/1000)}")
                last_sent_candle_timestamp = current_candle["timestamp"]

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
            print(f"Bougie partielle — O:{current_candle['open']} C:{current_candle['close']} V:{current_candle['volume']}")

def on_error(ws, error):
    print("Erreur WebSocket :", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket fermé :", close_msg)

# Boucle principale
if __name__ == "__main__": 
    kafka_producer = wait_for_kafka()

    # Lancer le timer_loop dans un thread séparé pour qu'il tourne sans contrainte
    timer_thread = threading.Thread(target=timer_loop, daemon=True)
    timer_thread.start()

    print("Démarrage du producer WebSocket...")
    websocket_url = f"wss://stream.binance.com:9443/ws/btcusdt@trade"

    ws = websocket.WebSocketApp(
        websocket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever() # Boucle 

        