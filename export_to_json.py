import json
from pymongo import MongoClient


def export_to_json_format():
    # 1. Connexion à  MongoDB locale
    client = MongoClient("mongodb://admin:Iloomph312@localhost:27017/")
    db = client['meteo_db']
    collection = db['stations']

    # 2. Récupération des données (on exclut l'ID interne MongoDB pour le JSON)
    stations = list(collection.find({}, {'_id': 0}))

    # 3. Préparation de la liste formatée
    final_output = []
    for s in stations:
        entry = {
            "Weather Station ID": s.get('id', 'N/A'),
            "Station Name": s.get('name', 'N/A'),
            "Latitude / Longitude": f"{s.get('latitude', 'N/A')}° N, {s.get('longitude', 'N/A')}° E",
            "Elevation": s.get('altitude', '23'),
            "City": s.get('name', 'N/A'),
            "State": "-/-",
            "Hardware": "other",
            "Software": "EasyWeatherPro_V5.1.6"
        }
        final_output.append(entry)

    # 4. Écriture du fichier JSON
    with open('rendu_final_stations.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)

    print(f"Fichier créé : rendu_final_stations.json avec {len(final_output)} stations.")


if __name__ == "__main__":
    export_to_json_format()
