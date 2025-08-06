#!/bin/bash

IP="52.209.87.79"

echo "API_URL_PREDICTION=http://${IP}:8000/predictions/by-date" > .env

docker compose build 

docker compose up -d

echo "Accès aux interfaces de ton projet :"
echo ""
echo "Kafka UI     : http://$IP:8080"
echo "PgAdmin      : http://$IP:5050"
echo "FastAPI      : http://$IP:8000/docs"
echo "Streamlit    : http://$IP:8501" 
echo ""
echo "Cliquer sur les liens"
echo ""








