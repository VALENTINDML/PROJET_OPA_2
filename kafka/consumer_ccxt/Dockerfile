FROM python:3.10-slim

WORKDIR /app

# Copie des dépendances
COPY requirements.txt .

# Installation des dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code
COPY consumer_ccxt_to_postgre.py .

# Commande d'exécution
CMD ["python", "consumer_ccxt_to_postgre.py"]