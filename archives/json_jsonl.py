import json

with open('InfoClimat.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

stations = data.get('stations', [])

with open('referentiel_stations.jsonl', 'w', encoding='utf-8') as f:
    for s in stations:
        f.write(json.dumps(s) + '\n')

print(f"Fichier créé : referentiel_stations.jsonl ({len(stations)} lignes)")
