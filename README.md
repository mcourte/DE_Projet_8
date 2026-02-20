# 🌦️ Pipeline ETL : Référentiel Stations Météo (Projet 8)

Ce projet automatise l'extraction, la transformation et le stockage du référentiel des stations météo d'InfoClimat. Le pipeline assure la transition des données depuis une source brute (Google Drive) vers une base de données NoSQL (MongoDB) via un transit sécurisé sur AWS S3.

## 🏗️ Architecture & Logique de Migration

Le processus suit une architecture **ETL** (Extract, Transform, Load) structurée :



### 1. Extraction (Source vers S3)
* **Source** : Fichier `referentiel_stations.jsonl` sur Google Drive.
* **Transport** : Synchronisation via **Airbyte** vers un bucket **AWS S3** (format CSV).
* **Formatage** : Les données sont extraites de la colonne brute `_airbyte_data`.

### 2. Transformation (Python & Pandas)
Le script `transform_and_load.py` effectue les opérations suivantes :
* **Nettoyage (Data Quality)** : Sur les 1157 lignes initiales, le script identifie et supprime les doublons.
* **Filtrage métier** : Seules les **4 stations de référence** sont conservées (élimination du bruit de mesures temporelles).
* **Validation** : Vérification de la présence des colonnes critiques (`id`, `name`, `latitude`, `longitude`).

### 3. Stockage & Réplication (MongoDB)
* **CRUD** : Le script implémente les opérations de création, lecture et mise à jour.
* **Réplication** : Les données sont stockées sur un **Replica Set** (3 nœuds via MongoDB Atlas ou config `--replSet`), garantissant la haute disponibilité et la tolérance aux pannes.

---

## 🚀 Fonctionnement du Script

### Installation
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Exécution du PipelineBash
```
python3 transform_and_load.py
```

### Génération du Rapport Final
```
python3 export_to_json.py
```

## 📊 Mesure de la Qualité (Post-Migration)


| Métrique | Valeur | Commentaire |
| :--- | :--- | :--- |
| **Lignes extraites (Raw)** | 1157 | Données brutes issues de S3 |
| **Stations validées** | 4 | Après filtrage et dédoublonnage |
| **Taux d'erreur/rejet** | 99.65% | Filtrage ciblé pour isoler le référentiel |
| **Statut final** | ✅ Succès | Migration conforme au schéma cible |

## 🛠️ Logigramme du Processus
**Début**: Lancement du script Python.
**Entrée** : Récupération du CSV sur AWS S3 via ```boto3```.
**Traitement** : Extraction JSON et nettoyage via ```pandas```.
**Décision** : Test d'intégrité (Doublons/Nulls).
**Stockage** : ```insert_many``` vers MongoDB (Replica Set).
**Fin** : Génération du JSON de rendu.

## 📝 Rapport Final : rendu_final_stations.json

Exemple du contenu attendu après exécution de ton script d'export.

```
[
    {
        "Weather Station ID": "07015",
        "Station Name": "Lille-Lesquin",
        "Latitude / Longitude": "50.57° N, 3.09° E",
        "Elevation": "47",
        "City": "Lille",
        "State": "-/-",
        "Hardware": "other",
        "Software": "EasyWeatherPro_V5.1.6"
    }
]
```