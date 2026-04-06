import os
import time
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

@st.cache_resource(show_spinner=False)
def init_connection():
    try:
        uri = st.secrets["MONGO_URI"]
    except KeyError:
        st.error("Identifiants MongoDB introuvables.")
        st.stop()
    return MongoClient(uri, tlsCAFile=certifi.where())

client = init_connection()
db = client['vacances_meteo']
collection = db['destinations']


def run_additional_queries():
    # 1. Classement des villes par cumul de précipitations (Villes les plus sèches)
    pipeline_secheresse = [
        {"$group": {
            "_id": "$nom",
            "total_precipitations": {"$sum": "$precipitations"}
        }},
        {"$sort": {"total_precipitations": 1}},
        {"$limit": 5}
    ]

    t0 = time.time()
    resultats_secheresse = list(collection.aggregate(pipeline_secheresse))
    t1 = time.time()

    print("Classement des 5 villes les plus sèches (cumul total) :")
    for res in resultats_secheresse:
        print(f" - {res['_id']}: {res['total_precipitations']:.2f} mm")
    print(f"Temps d'exécution : {t1 - t0:.4f} secondes\n")

    # 2. Analyse de la saisonnalité : Jours de beau temps (code 0 ou 1) par mois
    # Utilisation de $month pour extraire le mois de l'objet datetime BSON
    pipeline_saisonnalite = [
        {"$match": {"code_meteo": {"$in": [0, 1]}}},
        {"$project": {
            "mois": {"$month": "$date"}
        }},
        {"$group": {
            "_id": "$mois",
            "nombre_jours_soleil": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    t0 = time.time()
    resultats_saisonnalite = list(collection.aggregate(pipeline_saisonnalite))
    t1 = time.time()

    print("Distribution mensuelle des jours ensoleillés (National) :")
    for res in resultats_saisonnalite:
        print(f" - Mois {res['_id']}: {res['nombre_jours_soleil']} jours")
    print(f"Temps d'exécution : {t1 - t0:.4f} secondes\n")

    # 3. Records de température par région
    pipeline_records = [
        {"$group": {
            "_id": "$region",
            "temp_max_historique": {"$max": "$temperature"}
        }},
        {"$sort": {"temp_max_historique": -1}}
    ]

    t0 = time.time()
    resultats_records = list(collection.aggregate(pipeline_records))
    t1 = time.time()

    print("Records historiques de température par région :")
    for res in resultats_records:
        print(f" - {res['_id']}: {res['temp_max_historique']} C")
    print(f"Temps d'exécution : {t1 - t0:.4f} secondes\n")

    # 4. Stabilité thermique : Villes avec le moins de variations de température
    pipeline_stabilite = [
        {"$group": {
            "_id": "$nom",
            "ecart_type_temp": {"$stdDevPop": "$temperature"},
            "temp_moyenne": {"$avg": "$temperature"}
        }},
        {"$sort": {"ecart_type_temp": 1}},
        {"$limit": 5}
    ]

    t0 = time.time()
    resultats_stabilite = list(collection.aggregate(pipeline_stabilite))
    t1 = time.time()

    print("Top 5 des villes avec le climat le plus stable :")
    for res in resultats_stabilite:
        print(f" - {res['_id']}: Ecart-type {res['ecart_type_temp']:.2f} (Moyenne: {res['temp_moyenne']:.2f} C)")
    print(f"Temps d'exécution : {t1 - t0:.4f} secondes\n")

    # 5. Meilleure destination pour le mois de Juillet (Mois 7)
    # Utilisation de $month et filtrage sur l'entier 7
    pipeline_juillet = [
        {"$project": {
            "nom": 1,
            "temperature": 1,
            "mois": {"$month": "$date"}
        }},
        {"$match": {"mois": 7}},
        {"$group": {
            "_id": "$nom",
            "moyenne_juillet": {"$avg": "$temperature"}
        }},
        {"$sort": {"moyenne_juillet": -1}}
    ]

    t0 = time.time()
    resultats_juillet = list(collection.aggregate(pipeline_juillet))
    t1 = time.time()

    print("Classement des destinations pour le mois de Juillet :")
    for res in resultats_juillet:
        print(f" - {res['_id']}: {res['moyenne_juillet']:.2f} C")
    print(f"Temps d'exécution : {t1 - t0:.4f} secondes\n")


if __name__ == "__main__":
    try:
        run_additional_queries()
    finally:
        client.close()