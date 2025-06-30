# src/belibetl/pipelines/ingestion/nodes.py

import requests
from pymongo import MongoClient
import os
import certifi
from dotenv import load_dotenv

load_dotenv()

def fetch_api_data() -> list:
    """Récupère les données depuis l'API publique Belib."""
    url = os.getenv("API_URL")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json().get("results", [])
    print(f"🔎 Données brutes récupérées : {len(data)} éléments")
    if len(data) > 0:
        print(f"Exemple d'élément : {data[0]}")
    return data

def clean_data(data: list) -> list:
    """Nettoie les données en supprimant les éléments avec des champs essentiels manquants ou vides."""
    champs_essentiels = ["id_pdc_local", "statut_pdc", "nom_station"]

    cleaned = [
        record for record in data
        if all(record.get(champ) not in (None, "", "null") for champ in champs_essentiels)
    ]

    print(f"🧹 Données après nettoyage : {len(cleaned)} éléments (sur {len(data)} récupérés)")
    return cleaned

def insert_to_mongo(data: list) -> None:
    """Insère les données nettoyées dans MongoDB."""
    username = os.getenv('MONGO_USERNAME')
    password = os.getenv('MONGO_PASSWORD')
    dbname = os.getenv('MONGO_DBNAME')
    mongodb_uri_template = os.getenv('MONGO_URI')

    if not username or not password or not dbname or not mongodb_uri_template:
        raise ValueError("Les informations de connexion MongoDB sont manquantes dans les variables d'environnement.")

    mongodb_uri = mongodb_uri_template.replace("<db_password>", password)

    client = MongoClient(
        mongodb_uri,
        tls=True,
        tlsCAFile=certifi.where()
    )

    db = client[dbname]
    collection = db['clean_belib_data']

    try:
        if data:
            result = collection.insert_many(data)
            print(f"✅ {len(result.inserted_ids)} documents insérés dans MongoDB.")
        else:
            print("⚠️ Aucune donnée à insérer dans MongoDB.")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion dans MongoDB : {e}")
    finally:
        client.close()
