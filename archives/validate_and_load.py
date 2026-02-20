import pandas as pd
from pymongo import MongoClient


def validate_and_load(file_path):
    # 1. Chargement & Nettoyage
    df = pd.read_json(file_path, lines=True)

    print("Analyse de l'intégrité...")
    errors = []

    # Test : Valeurs manquantes sur les colonnes critiques
    if df['station_id'].isnull().any():
        errors.append("Erreur : ID de station manquant détecté.")

    # Test : Doublons
    if df['station_id'].duplicated().any():
        errors.append("Erreur : Doublons d'ID détectés.")

    if errors:
        for err in errors: print(err)
        return

    # 2. Migration
    client = MongoClient("mongodb://admin:Iloomph312@localhost:27017/")
    db = client['meteo_db']
    col = db['data-meteo']

    records = df.to_dict('records')
    result = col.insert_many(records)
    print(f"{len(result.inserted_ids)} documents insérés.")

    # 3. Test de réconciliation
    mongo_count = col.count_documents({})
    if mongo_count == len(df):
        print(" Succès : Le nombre de documents correspond au fichier source.")
    else:
        print(f" Alerte : Écart de données ({len(df)} vs {mongo_count})")


if __name__ == "__main__":
    validate_and_load('final_data_for_mongodb.json')
