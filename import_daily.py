import os
import json
import time
import streamlit as st
from dotenv import load_dotenv
import requests
from pymongo import MongoClient, DESCENDING
import certifi
from datetime import datetime, timedelta

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



def get_last_date_for_city(city_name):
    last_record = collection.find_one(
        {"nom": city_name},
        sort=[("date", DESCENDING)]
    )
    return last_record['date'] if last_record else None


def daily_update():
    try:
        with open('villes.json', 'r', encoding='utf-8') as f:
            villes = json.load(f)
    except Exception as e:
        print(f"Erreur JSON : {e}")
        return

    hier = datetime.now() - timedelta(days=2)
    end_date_str = hier.strftime("%Y-%m-%d")

    for ville in villes:
        last_date = get_last_date_for_city(ville['nom'])

        if last_date is None:
            start_date = datetime(1996, 1, 1)
        else:
            start_date = last_date + timedelta(days=1)

        if start_date > hier:
            print(f"Saut : {ville['nom']} a jour")
            continue

        start_date_str = start_date.strftime("%Y-%m-%d")
        print(f"Sync {ville['nom']} : {start_date_str} -> {end_date_str}")

        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={ville['lat']}&longitude={ville['lon']}&"
               f"start_date={start_date_str}&end_date={end_date_str}&"
               f"daily=temperature_2m_max,precipitation_sum,weathercode&timezone=auto")

        try:
            time.sleep(2)
            # Timeout augmente a 30s pour gerer le volume d'un an
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                data = response.json()
                daily = data.get('daily', {})
                dates = daily.get('time', [])

                if not dates: continue

                docs = []
                for i in range(len(dates)):
                    if daily['temperature_2m_max'][i] is None: continue
                    docs.append({
                        "nom": ville['nom'],
                        "region": ville['region'],
                        "latitude": ville['lat'],
                        "longitude": ville['lon'],
                        "date": datetime.strptime(dates[i], "%Y-%m-%d"),
                        "temperature": daily['temperature_2m_max'][i],
                        "precipitations": daily['precipitation_sum'][i],
                        "code_meteo": daily['weathercode'][i]
                    })

                if docs:
                    collection.insert_many(docs)
                    print(f"OK : {ville['nom']} (+{len(docs)} jours)")
            else:
                print(f"Erreur API {response.status_code} sur {ville['nom']}")

        except Exception as e:
            print(f"Erreur sur {ville['nom']} : {e}")


if __name__ == "__main__":
    daily_update()
    client.close()