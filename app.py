import streamlit as st
import pandas as pd
from pymongo import MongoClient
import certifi
import folium
from streamlit_folium import st_folium
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# --- 1. CONFIGURATION & CONNEXION ---
st.set_page_config(page_title="Travel Planner Big Data 2026", layout="wide")

WEATHER_MAP = {
    "Soleil": {"codes": [0, 1], "icon": "☀️"},
    "Nuageux": {"codes": [2, 3], "icon": "⛅"},
    "Pluie": {"codes": [51, 53, 55, 61, 63, 65, 80, 81, 82], "icon": "🌧️"},
    "Orage": {"codes": [95, 96, 99], "icon": "⚡"},
    "Neige": {"codes": [71, 73, 75, 77, 85, 86], "icon": "❄️"}
}


@st.cache_resource
def init_connection():
    # 1. On essaie de récupérer l'URI depuis les secrets Streamlit (Cloud)
    if "MONGO_URI" in st.secrets:
        uri = st.secrets["MONGO_URI"]
    else:
        # 2. Fallback pour le développement local si tu n'as pas configuré les secrets
        # Attention : ne mets ton vrai mot de passe ici que si ton repo est PRIVÉ
        # et que tu ne partages pas ce fichier.
        uri = "mongodb+srv://matisprrd_db_user:6xhVOfvmOGT03cQf@cluster0.ams8ehm.mongodb.net/?appName=Cluster0"

    return MongoClient(uri, tlsCAFile=certifi.where())


client = init_connection()
db = client['vacances_meteo']
collection = db['destinations']


# --- 2. FONCTIONS DE DONNÉES & PRÉDICTION ---

def get_prediction_for_city(city_name, month_idx):
    """Prédit la température pour 2026 en utilisant uniquement les données du mois choisi (1996-2025)."""
    pipeline = [
        {"$match": {
            "nom": city_name,
            "$expr": {
                "$and": [
                    {"$eq": [{"$month": "$date"}, month_idx]},
                    {"$lt": [{"$year": "$date"}, 2026]}  # On exclut 2026 pour ne pas fausser la tendance
                ]
            }
        }},
        {"$group": {
            "_id": {"$year": "$date"},
            "temp_moyenne": {"$avg": "$temperature"}
        }},
        {"$sort": {"_id": 1}}
    ]
    data = list(collection.aggregate(pipeline))
    if len(data) < 2: return None, None, None

    df_trend = pd.DataFrame(data).rename(columns={"_id": "Année", "temp_moyenne": "Temp"})

    # Régression Linéaire
    X = df_trend['Année'].values
    y = df_trend['Temp'].values
    coef = np.polyfit(X, y, 1)
    model = np.poly1d(coef)

    # Prédiction pour 2026
    pred_2026 = model(2026)
    return pred_2026, df_trend, model

@st.cache_data
def get_recommendations(month_idx, temp_range, selected_weathers):
    allowed_codes = []
    for w in selected_weathers:
        allowed_codes.extend(WEATHER_MAP[w]["codes"])

    target_temp = (temp_range[0] + temp_range[1]) / 2

    # Agrégation de base
    pipeline = [
        {"$match": {
            "$expr": {"$eq": [{"$month": "$date"}, month_idx]},
            "code_meteo": {"$in": allowed_codes}
        }},
        {"$group": {
            "_id": "$nom",
            "temp_hist": {"$avg": "$temperature"},  # Moyenne historique simple
            "precip_avg": {"$avg": "$precipitations"},
            "lat": {"$first": "$latitude"},
            "lon": {"$first": "$longitude"},
            "region": {"$first": "$region"}
        }}
    ]

    df = pd.DataFrame(list(collection.aggregate(pipeline)))

    if not df.empty:
        # --- INTÉGRATION DU SCORE PRÉDICTIF ---
        pred_temps = []
        for city in df['_id']:
            pred, _, _ = get_prediction_for_city(city, month_idx)
            pred_temps.append(pred if pred else 0)

        df['temp_predite_2026'] = pred_temps

        # Filtrage basé sur la température prédite (plus précis que l'historique brut)
        df = df[(df['temp_predite_2026'] >= temp_range[0]) & (df['temp_predite_2026'] <= temp_range[1])]

        if not df.empty:
            # Calcul du score : Proximité avec la prédiction 2026
            df['temp_diff'] = abs(df['temp_predite_2026'] - target_temp)
            max_diff = df['temp_diff'].max() if df['temp_diff'].max() != 0 else 1

            df['score'] = (1 - (df['temp_diff'] / max_diff)) * 80 + (1 / (df['precip_avg'] + 1)) * 20
            df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
            df.index += 1

    return df


# --- 3. INTERFACE UTILISATEUR ---

st.title("🌍 Destination Idéale 2026 (Analyse Prédictive)")

with st.sidebar:
    st.header("🔍 Filtres")
    mois_noms = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre",
                 "Novembre", "Décembre"]
    mois_sel = st.selectbox("Mois du voyage", range(1, 13), format_func=lambda x: mois_noms[x - 1])
    temp_sel = st.slider("Température souhaitée (°C)", -5, 40, (18, 28))
    weather_sel = st.multiselect("Météo", options=list(WEATHER_MAP.keys()), default=["Soleil", "Nuageux"])

df_results = get_recommendations(mois_sel, temp_sel, weather_sel)

# --- 4. CLASSEMENT & TOP CHOIX ---

if not df_results.empty:
    top_vane = df_results.iloc[0]

    st.success(f"### 🏆 Recommandation n°1 pour {mois_noms[mois_sel - 1]} 2026 : **{top_vane['_id']}**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Score de match", f"{top_vane['score']:.1f}/100")
    c2.metric("Temp. Prédite (2026)", f"{top_vane['temp_predite_2026']:.1f} °C",
              help="Calculé par régression linéaire sur 30 ans")
    c3.metric("Précipitations", f"{top_vane['precip_avg']:.2f} mm/j")

    st.divider()

    st.subheader("📍 Localisation")
    m = folium.Map(location=[top_vane['lat'], top_vane['lon']], zoom_start=5, tiles="cartodbpositron")
    for idx, row in df_results.iterrows():
        folium.Marker(
            [row['lat'], row['lon']],
            popup=f"{row['_id']} (Prédit: {row['temp_predite_2026']:.1f}°C)",
            icon=folium.Icon(color="orange" if idx == 1 else "blue")
        ).add_to(m)
    st_folium(m, use_container_width=True, height=500,returned_objects=[])

    st.subheader("📋 Classement Complet (Basé sur Prédictions)")
    display_df = df_results[['_id', 'region', 'temp_predite_2026', 'score']].copy()
    display_df.columns = ['Ville', 'Région', 'Temp. Prédite 2026', 'Score']
    st.table(display_df)

    # --- 5. ANALYSE PRÉDICTIVE DÉTAILLÉE ---
    st.divider()
    st.subheader(f"📈 Tendance du mois de {mois_noms[mois_sel - 1]} (1996-2026)")

    ville_focus = st.selectbox("Analyser la tendance d'une ville précise :", options=df_results['_id'])
    pred_val, df_trend, model = get_prediction_for_city(ville_focus, mois_sel)

    if df_trend is not None:
        X = df_trend['Année'].values
        y = df_trend['Temp'].values
        X_full = np.append(X, 2026)  # On ajoute 2026 pour le tracé

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=X, y=y, name=f"Moyenne {mois_noms[mois_sel - 1]}", mode='lines+markers'))
        fig.add_trace(
            go.Scatter(x=X_full, y=model(X_full), name="Tendance Long Terme", line=dict(color='red', dash='dash')))
        fig.add_trace(go.Scatter(x=[2026], y=[pred_val], name="Prédiction 2026",
                                 marker=dict(color='green', size=12, symbol='star')))

        fig.update_layout(xaxis_title="Année", yaxis_title="Température (°C)")
        st.plotly_chart(fig, use_container_width=True)

        st.info(
            f"💡 Pour **{ville_focus}**, on observe une tendance à {('la hausse' if model.coef[0] > 0 else 'la baisse')} pour le mois de {mois_noms[mois_sel - 1]}.")

else:
    st.warning("⚠️ Aucune destination ne correspond à vos critères prédictifs. Essayez d'ajuster les filtres.")