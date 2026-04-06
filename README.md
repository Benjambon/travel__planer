# Travel Planner Big Data 2026

Application web d'analyse de données météorologiques pour la recommandation de destinations de voyage.

**Application déployée :** [Lien vers l'application](https://travelplaner-zbx4hfmxan49srm3z3daut.streamlit.app/)

## Architecture du projet

* **app.py** : Programme principal de l'application web Streamlit.
* **import_data.py** : Script d'importation massive pour l'initialisation de la base de données MongoDB.
* **import_daily.py** : Script d'actualisation quotidienne des données (à utiliser via un planificateur de tâches).
* **requete.py** : Script de test des requêtes et d'évaluation des performances.
* **villes.json** : Données géographiques de référence des destinations.

## Prérequis

Installation des dépendances :

```bash
pip install -r requirements.txt


