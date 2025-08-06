**PROJET OPA**

*Lancement :*

- Aujouter son adresse IP dans la variable IP dans le fichier start.sh 
- Ensuite taper dans le terminal ./start.sh
- Ca fera automatiquement le build , le compose up en arrière plan et il y aura les URL de Pgadmin , Kafkaui , Streamlit et Fastapi qui s'afficheront
- Le chargement des pages web peut mettre 1 min

*Connexion à POSTGRES :*

mail : data@data.com 
mot de passe : data 
serveur : OPA (Peu importe le nom pour le moment)
nom d'hote : postgres
port : 5432 
identifiant : data 
mot de passe : data
base de données de maintenance : postgres
base de données : data

Table : 

ccxt_ohlcv
prediction_history
latest_prediction






*Structure du projet : *

-PROJET_OPA_2
    - fastapi
        - app.py    *Script FastApi
        - Dockerfile
        - requirements.txt
    - kafka
        - consumer_ccxt
            - consumer_ccxt_to_postgre.py    *Récupère dans le topic kafka les OHLCV ccxt pour insertion dans la table ccxt_ohlcv
            - Dockerfile
            - requirements.txt
        - consuler_ws
            - consumer_websocket_to_predict.py    *Crée les tables prediction_history et latest_prediction, applique le model.pkl et insère dans les tables
            - Dockerfile
            - requirements.txt
        - producer_ccxt 
            - Dockerfile
            - producer_ccxt.py   *Récupère les bougies OHLCV de 5min en batch et envoie dans le topic Kafka
            - requirements.txt
        - producer_ws 
            - Dockerfile
            - producer_websocket.py  *Traite en temps réel les trades, envoie les OHLCV de 5min à Kafka, recommence le process 
            - requirements.txt 
    - ml 
        - Dockerfile
        - requirements.txt
        - train_model.py *Entrainement du model,sur les données historique de la table ccxt_ohlcv, pour prédire la montée du prix dans 5min et donc *l'achat ou non
    - models
        -model.pkl
    - steamlit
        - Dockerfile
        -requirements.txt
        - streamlit.py   *Visualitation de la prédiction, affichage des prédictions sur les 30 dernières minutes
    - .env  *URL de l'endpoint API avec mise à jour à chaque lancement de l'adresse IP
    - docker-compose.yml  
    - README.md
    - start.sh   *Script pour le lancement du projet. C'est ici que se met à jour l'IP


