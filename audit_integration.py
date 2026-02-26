import pandas as pd
from pymongo import MongoClient
import boto3
import io
import json
import os
from dotenv import load_dotenv

# 1. Chargement des variables d'environnement (Secrets AWS)
load_dotenv()

# 2. Configuration
S3_BUCKET = 'ocr-8-meteo'
PREFIX = 'data/ocr-8-meteo/info_climat/referentiel_station/'
MONGO_URI = "mongodb://admin:Iloomph312@localhost:27017/"
DB_NAME = "meteo_db"
COLLECTION_NAME = "stations"

def get_latest_s3_file(s3_client):
    """Trouve le fichier le plus récent dans le dossier S3 pour l'audit"""
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=PREFIX)
    files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
    return files[0]['Key'] if files else None

def get_integrity_metrics(df, label="SOURCE"):
    """Calcule les métriques d'intégrité demandées"""
    print(f"\n--- 📊 Analyse d'intégrité : {label} ---")
    
    # Colonnes disponibles
    cols = list(df.columns)
    print(f"✅ Colonnes présentes : {cols}")
    
    # Doublons (basés sur l'ID de la station)
    dup_count = df.duplicated(subset=['id']).sum() if 'id' in df.columns else 0
    print(f"👯 Doublons détectés : {dup_count}")
    
    # Valeurs manquantes sur les champs critiques
    critical_fields = ['id', 'name', 'latitude', 'longitude']
    existing_critical = [f for f in critical_fields if f in df.columns]
    missing = df[existing_critical].isnull().sum().sum()
    print(f"❓ Valeurs manquantes (champs clés) : {missing}")
    
    # Types des variables
    if 'latitude' in df.columns and 'id' in df.columns:
        print(f"🧬 Types : ID({df['id'].dtype}), Lat({df['latitude'].dtype})")
    
    return {"count": len(df), "errors": dup_count + missing}

def run_audit():
    # Initialisation AWS avec les secrets du .env
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-west-3')
    )

    # --- PHASE 1 : AVANT MIGRATION (S3) ---
    latest_key = get_latest_s3_file(s3)
    if not latest_key:
        print("❌ Erreur : Aucun fichier trouvé sur S3.")
        return

    print(f"📥 Lecture du fichier S3 : {latest_key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)
    df_raw = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    # Extraction des données JSON Airbyte si nécessaire
    if '_airbyte_data' in df_raw.columns:
        df_source = pd.DataFrame([json.loads(x) for x in df_raw['_airbyte_data']])
    else:
        df_source = df_raw
    
    metrics_before = get_integrity_metrics(df_source, "AVANT (S3 - Données Brutes)")

    # --- PHASE 2 : APRÈS MIGRATION (MongoDB) ---
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    # On récupère les données de Mongo sans le champ technique _id
    df_mongo = pd.DataFrame(list(db[COLLECTION_NAME].find({}, {'_id': 0})))
    
    if df_mongo.empty:
        print("\n❌ Erreur : La collection MongoDB est vide.")
        return

    metrics_after = get_integrity_metrics(df_mongo, "APRÈS (MongoDB - Données Finales)")

    # --- BILAN FINAL ---
    print("\n" + "="*50)
    print("🏆 BILAN DE LA MIGRATION")
    print(f"📈 Stations en entrée : {metrics_before['count']}")
    print(f"📉 Stations en sortie : {metrics_after['count']} (après filtrage et dédoublonnage)")
    
    error_rate = (metrics_after['errors'] / (metrics_after['count'] * 4)) * 100
    print(f"🎯 Taux d'erreur post-migration : {error_rate:.2f}%")
    
    if error_rate == 0:
        print("✅ SUCCESS : L'intégrité des données est parfaite.")
    else:
        print("⚠️ WARNING : Des anomalies ont été détectées en base.")
    print("="*50)

if __name__ == "__main__":
    run_audit()