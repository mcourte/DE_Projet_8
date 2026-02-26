from pymongo import MongoClient
import time

# Utilisation de l'URI avec le paramètre replicaSet si configuré
MONGO_URI = "mongodb://admin:Iloomph312@localhost:27017/?replicaSet=rs0"

def verify_replication():
    client = MongoClient(MONGO_URI)
    db = client['meteo_db']
    collection = db['stations']

    print("--- 🔄 TEST DE RÉPLICATION MONGODB ---")

    # 1. Vérification de l'état du Replica Set
    status = client.admin.command("replSetGetStatus")
    members = status['members']
    print(f"Membres du Replica Set détectés : {len(members)}")
    for m in members:
        print(f" - Hôte: {m['name']} | État: {m['stateStr']}")

    # 2. Mise à jour de test (sur le Primary)
    test_id = "07015"
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n📝 Mise à jour de la station {test_id} à {timestamp}...")
    
    collection.update_one(
        {"id": test_id},
        {"$set": {"last_replication_test": timestamp}}
    )

    # 3. Pause courte pour laisser la réplication agir (en ms)
    time.sleep(1)

    # 4. Lecture de vérification
    updated_doc = collection.find_one({"id": test_id})
    if updated_doc.get("last_replication_test") == timestamp:
        print("✅ SUCCÈS : La mise à jour a été propagée et lue avec succès.")
    else:
        print("❌ ÉCHEC : La donnée lue ne correspond pas à la mise à jour.")

if __name__ == "__main__":
    verify_replication()