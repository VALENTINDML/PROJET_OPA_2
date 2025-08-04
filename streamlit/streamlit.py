import streamlit as st
import requests
import time
import os

st.set_page_config(page_title="Prédictions BTC/USDT", layout="centered")

st.title("📈 Prédiction BTC/USDT (toutes les 5 minutes)")
st.markdown("Prédictions en temps réel à partir du modèle ML via WebSocket Binance")

placeholder = st.empty()

API_URL = os.environ.get("API_URL", "http://34.246.202.63:8000/predict/")

#API_URL = "http://localhost:8000/predict/"  # adapte si ton FastAPI est ailleurs

def fetch_prediction():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Erreur {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}

# Rafraîchissement automatique toutes les 5 secondes
while True:
    result = fetch_prediction()

    with placeholder.container():
        st.subheader("🧠 Dernière prédiction")
        if "prediction" in result:
            pred = result["prediction"]
            if pred == 1:
                st.success("✅ Signal d'ACHAT (1)")
            elif pred == 0:
                st.warning("⛔ Aucun signal d'achat (0)")
            else:
                st.info(f"Résultat brut : {pred}")
        elif "status" in result:
            st.info(result["status"])
        else:
            st.error(f"Erreur : {result.get('error', 'inconnue')}")

    time.sleep(5)

