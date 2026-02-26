import time
from pymongo import MongoClient

# Configuration
MONGO_URI = "mongodb://admin:Iloomph312@localhost:27017/"
DB_NAME = "meteo_db"
COLLECTION_NAME = "stations"

def run_audit():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print("--- 🔍 AUDIT POST-MIGRATION ---")

    # 1. MESURE DU TEMPS D'ACCESSIBILITÉ (LATENCE)
    start_time = time.perf_counter()
    
    # On simule une lecture complète de la base
    all_stations = list(collection.find({}))
    
    end_time = time.perf_counter()
    latency_ms = (end_time - start_time) * 1000
    
    print(f"⏱️ Temps d'accessibilité : {latency_ms:.2f} ms")

    # 2. MESURE DU TAUX DE CONFORMITÉ (SCHÉMA)
    total_docs = len(all_stations)
    if total_docs == 0:
        print("❌ Erreur : La collection est vide.")
        return

    invalid_count = 0
    # Définition du "Contrat de donnée" (champs obligatoires)
    required_fields = {'id', 'name', 'latitude', 'longitude'}

    for doc in all_stations:
        # Check 1: Présence des champs
        if not required_fields.issubset(doc.keys()):
            invalid_count += 1
            continue
        
        # Check 2: Validité des types (optionnel mais recommandé)
        if not isinstance(doc['latitude'], (int, float, str)) or not doc['id']:
            invalid_count += 1

    error_rate = (invalid_count / total_docs) * 100

    print(f"📊 Nombre de documents analysés : {total_docs}")
    print(f"✅ Taux de documents non-conformes : {error_rate:.2f}%")
    
    if error_rate == 0:
        print("🚀 Résultat : Schéma 100% valide.")
    else:
        print(f"⚠️ Alerte : {invalid_count} documents corrompus détectés.")

if __name__ == "__main__":
    run_audit()