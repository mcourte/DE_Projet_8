import pandas as pd
from pymongo import MongoClient
import boto3
import io
import json
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3')
# Configuration
S3_BUCKET = 'ocr-8-meteo'
PREFIX = 'data/ocr-8-meteo/info_climat/referentiel_station/'
MONGO_URI = "mongodb://admin:Iloomph312@localhost:27017/"


def get_latest_s3_file(s3_client):
    """Trouve le fichier le plus récent dans le dossier S3"""
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=PREFIX)
    files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
    return files[0]['Key'] if files else None


def transform_and_load():
    latest_key = get_latest_s3_file(s3)
    print(f"Traitement du fichier : {latest_key}")

    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Extraction des données brutes Airbyte
    if '_airbyte_data' in df.columns:
        data_list = [json.loads(row) for row in df['_airbyte_data']]
        df_final = pd.DataFrame(data_list)
    else:
        df_final = df

    # --- TESTS D'INTÉGRITÉ AUTOMATISÉS ---
    print("Exécution des tests d'intégrité...")

    # 1. Test des doublons
    initial_count = len(df_final)
    df_final = df_final.drop_duplicates(subset=['id'])
    if len(df_final) < initial_count:
        print(f"{initial_count - len(df_final)} doublons supprimés.")

    # 2. Test des colonnes obligatoires
    cols_attendues = {'id', 'name', 'latitude', 'longitude'}
    missing = cols_attendues - set(df_final.columns)
    assert not missing, f"Colonnes manquantes : {missing}"

    # 3. Test du nombre de stations
    df_final = df_final[df_final['id'].str.len() <= 10]
    print(f" Nombre de stations validées : {len(df_final)}")

    # Chargement MongoDB
    client = MongoClient(MONGO_URI)
    db = client['meteo_db']
    collection = db['stations']

    collection.delete_many({})
    records = df_final.to_dict("records")
    collection.insert_many(records)
    print(f" {len(records)} stations migrées avec succès vers MongoDB.")


def crud_demonstration(collection):
    print("\n Démonstration CRUD (Read & Update) :")

    # 1. READ
    station = collection.find_one({"id": "07015"})
    if station:
        print(f"   Lecture : La station 07015 s'appelle {station['name']}")

    # 2. UPDATE
    result = collection.update_one(
        {"id": "07015"},
        {"$set": {"statut": "opérationnel", "derniere_maj": "2026-02-20"}}
    )
    if result.modified_count > 0:
        print("   Mise à jour : Le statut de la station a été modifié.")


if __name__ == "__main__":
    # 1. Lancement de l'ETL
    transform_and_load()

    # 2. Vérification CRUD (Lecture/Mise à jour)
    client = MongoClient(MONGO_URI)
    db = client['meteo_db']
    collection = db['stations']
    crud_demonstration(collection)
