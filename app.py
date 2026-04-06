import streamlit as st
import pandas as pd
from pymongo import MongoClient
import certifi
import numpy as np
from datetime import datetime

st.set_page_config(page_title="Travel Planner Big Data 2026", layout="wide")

st.markdown("""
    <meta name="description" content="Planificateur de voyage utilisant le Big Data pour prédire la météo 2026.">
    <style>
        .block-container { min-height: 100vh; }
    </style>
""", unsafe_allow_html=True)

WEATHER_MAP = {
    "Soleil": {"codes": [0, 1], "icon": "☀️"},
    "Nuageux": {"codes": [2, 3], "icon": "⛅"},
    "Pluie": {"codes": [51, 53, 55, 61, 63, 65, 80, 81, 82], "icon": "🌧️"}
}


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


@st.cache_data(ttl=3600, show_spinner=False)
def get_base_predictions(month_idx, selected_weathers):
    allowed_codes = []
    for w in selected_weathers:
        allowed_codes.extend(WEATHER_MAP[w]["codes"])

    pipeline = [
        {"$match": {
            "$expr": {"$eq": [{"$month": "$date"}, month_idx]},
            "date": {"$lt": datetime(2026, 1, 1)}
        }},
        {"$group": {
            "_id": "$nom",
            "lat": {"$first": "$latitude"},
            "lon": {"$first": "$longitude"},
            "region": {"$first": "$region"},
            "precip_avg": {"$avg": "$precipitations"},
            "history": {"$push": {"year": {"$year": "$date"}, "temp": "$temperature"}},
            "jours_meteo_ok": {
                "$sum": {"$cond": [{"$in": ["$code_meteo", allowed_codes]}, 1, 0]}
            }
        }},
        {"$match": {"jours_meteo_ok": {"$gt": 0}}}
    ]

    raw_data = list(collection.aggregate(pipeline))
    if not raw_data: return []

    results = []
    for city in raw_data:
        hist_df = pd.DataFrame(city['history']).groupby('year').mean().reset_index()
        if len(hist_df) >= 2:
            coef = np.polyfit(hist_df['year'], hist_df['temp'], 1)
            pred_2026 = np.poly1d(coef)(2026)

            results.append({
                "Ville": city['_id'],
                "Région": city['region'],
                "lat": city['lat'],
                "lon": city['lon'],
                "Temp_2026": round(pred_2026, 1),
                "precip_avg": city['precip_avg'],
                "coef": coef,
                "history": hist_df
            })
    return results


def filter_and_score(base_data, temp_range):
    if not base_data: return pd.DataFrame()

    target_temp = sum(temp_range) / 2
    filtered = []

    for city in base_data:
        if temp_range[0] <= city["Temp_2026"] <= temp_range[1]:
            temp_diff = abs(city["Temp_2026"] - target_temp)

            raw_score = (100 - (temp_diff * 3)) + (1 / (city['precip_avg'] + 1))
            final_score = max(0.0, min(100.0, raw_score))

            city_copy = city.copy()
            city_copy["Score"] = round(final_score, 1)
            filtered.append(city_copy)

    df = pd.DataFrame(filtered).sort_values(by="Score", ascending=False).reset_index(drop=True)
    if not df.empty: df.index += 1
    return df


st.title("🌍 Destination Idéale 2026")

with st.sidebar:
    st.header("🔍 Filtres")
    mois_noms = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre",
                 "Novembre", "Décembre"]

    with st.form("search_form"):
        mois_sel = st.selectbox("Mois du voyage", range(1, 13), format_func=lambda x: mois_noms[x - 1])
        temp_sel = st.slider("Température souhaitée (°C)", -5, 40, (18, 28))
        weather_sel = st.multiselect(
            "Météo",
            options=list(WEATHER_MAP.keys()),
            default=["Soleil", "Nuageux"],
            format_func=lambda x: f"{WEATHER_MAP[x]['icon']} {x}"
        )
        submitted = st.form_submit_button("Lancer la recherche 🚀")

    with st.spinner('Analyse Big Data en cours...'):
        base_data = get_base_predictions(mois_sel, weather_sel)

df_results = filter_and_score(base_data, temp_sel)

if not df_results.empty:
    top_vane = df_results.iloc[0]

    st.success(f"### Recommandation n°1 pour {mois_noms[mois_sel - 1]} 2026 : **{top_vane['Ville']}**")

    c1, c2, c3 = st.columns(3)
    c1.metric("Score de match", f"{top_vane['Score']}/100")
    c2.metric("Temp. Prédite (2026)", f"{top_vane['Temp_2026']} °C")
    precip_mensuelle = top_vane['precip_avg'] * 30
    c3.metric("Précipitations", f"{precip_mensuelle:.0f} mm/mois")

    st.divider()

    st.subheader("Classement Complet")
    display_df = df_results[['Ville', 'Région', 'Temp_2026', 'Score']].copy()

    st.dataframe(
        display_df,
        use_container_width=True,
        height=400,
        column_config={
            "Ville": st.column_config.TextColumn(width="medium"),
            "Région": st.column_config.TextColumn(width="large"),
            "Temp_2026": st.column_config.NumberColumn(width="small"),
            "Score": st.column_config.NumberColumn(width="small")
        }
    )

    st.divider()

    st.subheader("Carte des destinations")

    if st.button("🗺️ Afficher la carte Folium", key="btn_map"):
        with st.spinner("Génération de la carte..."):
            import folium
            from streamlit_folium import st_folium

            m = folium.Map(location=[top_vane['lat'], top_vane['lon']], zoom_start=5, tiles="cartodbpositron")

            for idx, row in df_results.head(20).iterrows():
                folium.Marker(
                    [row['lat'], row['lon']],
                    popup=f"{row['Ville']} ({row['Temp_2026']}°C)",
                    icon=folium.Icon(color="orange" if idx == 1 else "blue")
                ).add_to(m)
            st_folium(m, use_container_width=True, height=500, returned_objects=[])

    st.divider()

    st.subheader(f"📈 Tendance Historique du mois de {mois_noms[mois_sel - 1]}")
    ville_focus = st.selectbox("Analyser la tendance d'une ville précise :", options=df_results['Ville'])

    if st.button("📊 Générer le graphique interactif", key="btn_chart"):
        with st.spinner("Calcul des tendances..."):
            import plotly.graph_objects as go

            city_data = df_results[df_results['Ville'] == ville_focus].iloc[0]
            h_df = city_data['history']
            model = np.poly1d(city_data['coef'])

            X = h_df['year'].values
            y = h_df['temp'].values
            X_full = np.append(X, 2026)

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=X, y=y, name="Historique", mode='lines+markers'))
            fig.add_trace(go.Scatter(x=X_full, y=model(X_full), name="Tendance", line=dict(color='red', dash='dash')))
            fig.add_trace(go.Scatter(x=[2026], y=[city_data['Temp_2026']], name="Prédiction",
                                     marker=dict(color='green', size=12, symbol='star')))

            fig.update_layout(xaxis_title="Année", yaxis_title="Température (°C)", margin=dict(l=0, r=0, t=30, b=0),
                              height=400)
            st.plotly_chart(fig, use_container_width=True)

else:
    st.warning("Aucune destination ne correspond à vos critères prédictifs. Essayez d'ajuster la température.")