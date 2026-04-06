import os
import json
import time
from dotenv import load_dotenv
import requests
from pymongo import MongoClient, ASCENDING
import certifi
from datetime import datetime

load_dotenv("mdp.env")
uri = os.getenv("MONGO_URI")

if not uri:
    raise ValueError("MONGO_URI introuvable")

client = MongoClient(uri, tlsCAFile=certifi.where())
db = client['vacances_meteo']
collection = db['destinations']


def setup_all_indexes():
    try:
        collection.create_index([("nom", ASCENDING)], name="idx_nom")
        collection.create_index([("region", ASCENDING)], name="idx_region")
        collection.create_index([("date", ASCENDING)], name="idx_date")
        collection.create_index(
            [("temperature", ASCENDING), ("precipitations", ASCENDING)],
            name="idx_meteo_critere"
        )
    except Exception as e:
        print(f"Erreur indexation : {e}")


def import_avec_dates_bson():
    try:
        with open('villes.json', 'r', encoding='utf-8') as f:
            villes = json.load(f)
    except Exception as e:
        print(f"Erreur JSON : {e}")
        return

    for ville in villes:
        if collection.find_one({"nom": ville['nom']}):
            print(f"Saut : {ville['nom']} deja en base")
            continue

        print(f"Requete en cours : {ville['nom']}")

        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={ville['lat']}&longitude={ville['lon']}&"
               f"start_date=1996-01-01&end_date=2024-01-01&"
               f"daily=temperature_2m_max,precipitation_sum,weathercode&timezone=auto")

        try:
            # Limitation du debit : pause de 2 secondes entre les villes
            time.sleep(2)

            response = requests.get(url)

            # Gestion du code 429 (Too Many Requests)
            if response.status_code == 429:
                print("Limite API atteinte, attente de 30 secondes...")
                time.sleep(30)
                response = requests.get(url)

            if response.status_code != 200:
                print(f"Erreur API {response.status_code} pour {ville['nom']}")
                continue

            data = response.json()
            daily = data.get('daily', {})

            dates_brutes = daily.get('time', [])
            temps_max = daily.get('temperature_2m_max', [])
            pluies = daily.get('precipitation_sum', [])
            codes = daily.get('weathercode', [])

            documents = []
            for i in range(len(dates_brutes)):
                if temps_max[i] is None:
                    continue

                documents.append({
                    "nom": ville['nom'],
                    "region": ville['region'],
                    "latitude": ville['lat'],
                    "longitude": ville['lon'],
                    "date": datetime.strptime(dates_brutes[i], "%Y-%m-%d"),
                    "temperature": temps_max[i],
                    "precipitations": pluies[i],
                    "code_meteo": codes[i]
                })

            if documents:
                collection.insert_many(documents)
                print(f"Succes : {ville['nom']} ({len(documents)} documents)")

        except Exception as e:
            print(f"Erreur sur {ville['nom']} : {e}")


if __name__ == "__main__":
    try:
        setup_all_indexes()
        import_avec_dates_bson()
    finally:
        client.close()
