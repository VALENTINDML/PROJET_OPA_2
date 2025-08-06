import streamlit as st
import requests
import time
import os
from datetime import datetime , timedelta

st.set_page_config(page_title="Prédictions BTC/USDT")

st.title("📈 Prédiction BTC/USDT (toutes les 5 minutes)")
st.markdown("Prédictions en temps réel à partir du modèle ML via WebSocket Binance")

placeholder = st.empty()


API_URL_PREDICTION = os.environ.get("API_URL_PREDICTION", "http://localhost:8000/predictions/by-date")


def fetch_prediction_date():
    try: 
        response = requests.get(API_URL_PREDICTION)
        if response.status_code == 200:
            return response.json()
        else: 
            return{"error": f"Erreur {response.status_code}"}
    except Exception as e:
            return {"error": str(e)}


while True:
    prediction_date = fetch_prediction_date()

    with placeholder.container():
    
        if not prediction_date:
            st.warning("Aucune donnée trouvée.")
        else:
            st.subheader("📅 Prédictions durant les 30 dernières minutes")
            for item in prediction_date[:6]:

                heure = item["timestamp"].split("T")[1][:5]

                dt = datetime.fromisoformat(item["timestamp"]) + timedelta(minutes=5)
                heure_5 = dt.strftime("%H:%M")

                pred = "✅ Achat" if item["prediction"] == 1 else "⛔ Pas d'achat"
                proba = item["proba_achat"]
                prix = item["last_price"]
                volume = item["volume"]
                
                with st.container():
                    st.write(f"### 🕒 Open {heure} - Close {heure_5} - {pred}")
                    st.write(f"💰 Prix de cloture bougies: {prix:,.2f} $")
                    st.write(f"📊 Probabilité d'achat : {proba:.0%}")
                    st.write(f"📦 Volume sur les 5 dernières minutes : {volume:.2f}")
                    st.markdown("---")

# Rafraichissement auto toutes les 5 secondes
    time.sleep(60)

