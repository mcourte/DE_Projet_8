import pandas as pd
from pymongo import MongoClient
import json

# Configuration
MONGO_URI = "mongodb://admin:Iloomph312@localhost:27017/"
DB_NAME = "meteo_db"
COLLECTION_NAME = "stations"
SOURCE_FILE = "InfoClimat.json" # Ton fichier brut avant transformation

def run_full_integrity_check():
    print("=== 🛡️ RAPPORT D'INTÉGRITÉ & QUALITÉ DES DONNÉES ===")

    # --- 1. ANALYSE AVANT MIGRATION (Source) ---
    print("\n[Phase 1] Audit du fichier source...")
    try:
        with open(SOURCE_FILE, 'r') as f:
            raw_data = json.load(f)
            # On simule le chargement brut pour compter
            stations_brutes = raw_data.get('stations', [])
            print(f"   - Nombre de lignes brutes détectées : {len(stations_brutes)}")
    except Exception as e:
        print(f"   - ⚠️ Impossible d'analyser la source : {e}")

    # --- 2. ANALYSE APRÈS MIGRATION (MongoDB) ---
    print("\n[Phase 2] Audit de la base MongoDB (Post-Migration)...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    cursor = list(collection.find({}))
    total_docs = len(cursor)
    
    if total_docs == 0:
        print("   ❌ Erreur : Aucun document en base.")
        return

    # Indicateurs de qualité
    metrics = {
        "missing_fields": 0,
        "type_errors": 0,
        "duplicates": 0,
        "invalid_coords": 0
    }

    seen_ids = set()
    required_fields = ['id', 'name', 'latitude', 'longitude']

    for doc in cursor:
        # A. Test des Doublons (ID unique)
        if doc['id'] in seen_ids:
            metrics["duplicates"] += 1
        seen_ids.add(doc['id'])

        # B. Test des Valeurs Manquantes / Champs obligatoires
        for field in required_fields:
            if field not in doc or doc[field] is None or doc[field] == "":
                metrics["missing_fields"] += 1

        # C. Test des Types de variables
        # Latitude/Longitude doivent être des nombres (float/int) ou convertibles
        try:
            lat = float(doc.get('latitude', 'error'))
            lon = float(doc.get('longitude', 'error'))
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                metrics["invalid_coords"] += 1
        except (ValueError, TypeError):
            metrics["type_errors"] += 1

    # --- 3. CALCUL DU TAUX D'ERREUR FINAL ---
    total_errors = sum(metrics.values())
    # On calcule le taux par rapport au nombre de champs vérifiés (total_docs * nb_regles)
    error_rate = (total_errors / (total_docs * 4)) * 100 

    print(f"   - Documents analysés : {total_docs}")
    print(f"   - Doublons trouvés : {metrics['duplicates']}")
    print(f"   - Valeurs manquantes : {metrics['missing_fields']}")
    print(f"   - Erreurs de types/coordonnées : {metrics['type_errors'] + metrics['invalid_coords']}")
    
    print(f"\n--- 📊 SCORE DE QUALITÉ FINAL ---")
    print(f"➡️ Taux d'erreurs post-migration : {error_rate:.2f}%")
    
    if error_rate == 0:
        print("✅ Intégrité Totale : Les données sont conformes au schéma cible.")
    else:
        print(f"⚠️ Qualité dégradée : {total_errors} anomalies détectées.")

if __name__ == "__main__":
    run_full_integrity_check()