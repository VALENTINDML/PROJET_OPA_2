import psycopg2
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib
import os
import time
import sklearn

# Paramètres de connexion PostgreSQL
POSTGRES_DB = os.environ.get("POSTGRES_DB", "binance_data")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")

# Connexion PostgreSQL
#conn = psycopg2.connect(
#    dbname=POSTGRES_DB,
#    user=POSTGRES_USER,
#    password=POSTGRES_PASSWORD,
#    host=POSTGRES_HOST,
#    port=POSTGRES_PORT
#)


#max_retries = 10
#retry_delay = 5  # secondes

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

conn = wait_for_postgres()

# Lecture des données
df = pd.read_sql("SELECT * FROM binance_ohlcv_5m ORDER BY timestamp ASC", conn)
conn.close()

# Transformation du timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'] , unit ="ms")

# Création de la cible "achat" = 1 si close[t+1] > close[t], sinon 0
df['future_close'] = df['close'].shift(-1)
df['target'] = (df['future_close'] > df['close']).astype(int)

# Suppression des dernières lignes sans target
df.dropna(inplace=True)

# Caractéristiques d’entrée
features = ['open', 'high', 'low', 'close', 'volume']
X = df[features]
y = df['target']

# Split train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Entraînement du modèle
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Évaluation
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred))

os.makedirs("./models", exist_ok=True)

# Sauvegarde du modèle
joblib.dump(model, "./models/model.pkl")
print("✅ Modèle sauvegardé sous models/model.pkl")

