FROM python:3.10-slim

WORKDIR /app

# Copie des dépendances
COPY requirements.txt .

# Installation des dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code
COPY app.py .

# Commande d'exécution
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]