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
POSTGRES_DB = os.environ.get("POSTGRES_DB", "data")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "data")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "data")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")

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
df = pd.read_sql("SELECT * FROM ccxt_ohlcv ORDER BY timestamp ASC", conn)
conn.close()

# Transformation du timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'] , unit ="ms")

# Création de la cible "achat" = 1 si close[t+1] > close[t], sinon 0
df['future_close'] = df['close'].shift(-1)
df['target'] = (df['future_close'] > df['close']).astype(int)

"""df['close'] contient les prix de clôture actuels pour chaque période.
La méthode .shift(-1) décale la colonne vers le haut d'une ligne, donc pour chaque ligne, future_close contient le prix de clôture de la période suivante.

La colonne target sera la variable à prédire , 
Elle compare si le prix de clôture futur (future_close) est plus grand que le prix de clôture actuel (close).

Le modele vise à prédire si le prix va augmenter ou non dans la prochaine timelaps de 5min 
et donc déclencher un achat ou non """

# Suppression des dernières lignes sans target
df.dropna(inplace=True)

# Caractéristiques d'entrée d'entrainement du modèle
features = ['open', 'high', 'low', 'close', 'volume']
X = df[features]
y = df['target']

# Split train/test 80/20
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
print("Modèle sauvegardé sous models/model.pkl")

