# 🌦️ Pipeline ETL : Référentiel Stations Météo (Projet 8)

Ce projet automatise l'extraction, la transformation et le stockage du référentiel des stations météo d'InfoClimat. Le pipeline assure la transition sécurisée des données depuis une source brute vers une base de données NoSQL MongoDB configurée en haute disponibilité.

## 🏗️ Architecture & Logique de Migration

Le processus suit une architecture **ETL** (Extract, Transform, Load) structurée pour garantir l'intégrité des données :



### 1. Extraction (Source vers S3)
* **Source** : Données brutes issues d'InfoClimat (via Google Drive).
* **Transport** : Synchronisation via **Airbyte** vers un bucket **AWS S3** (format CSV).
* **Formatage** : Les données sont extraites dynamiquement depuis la colonne brute `_airbyte_data`.

### 2. Transformation (Python & Pandas)
Le script `transform_and_load.py` effectue les opérations critiques suivantes :
* **Nettoyage (Data Quality)** : Sur les 1157 lignes initiales, le script identifie et supprime les doublons (1152 lignes redondantes éliminées).
* **Filtrage métier** : Seules les **4 stations de référence** sont conservées pour isoler le référentiel du bruit de mesures.
* **Validation** : Vérification stricte du typage (coordonnées en floats) et de la présence des colonnes critiques (`id`, `name`, `latitude`, `longitude`).

### 3. Stockage & Réplication (MongoDB)
* **CRUD** : Implémentation des opérations de création, lecture et mise à jour.
* **Haute Disponibilité** : Les données sont stockées sur un **Replica Set** (`rs0`), garantissant la tolérance aux pannes et la persistance des données sur plusieurs nœuds.

---

## 🔍 Observabilité & Qualité des Données

Afin de valider la robustesse du pipeline, trois scripts d'audit ont été déployés :

### 🛡️ Audit d'Intégrité (`audit_integrity.py`)
| Indicateur | Source (S3) | Cible (MongoDB) | Statut |
| :--- | :--- | :--- | :--- |
| **Volume de lignes** | 1157 | **4** | ✅ Filtrage OK |
| **Doublons (IDs)** | 1152 | **0** | ✅ Dédoublonnage OK |
| **Valeurs manquantes**| 4580 | **0** | ✅ Nettoyage OK |
| **Taux d'erreur final**| - | **0.00%** | ✅ Intégrité Totale |



### ⏱️ Mesure de Performance (`temp_access.py`)
* **Temps d'accessibilité moyen** : **45.56 ms**.
* **Analyse** : Ce temps de réponse quasi-instantané valide l'indexation et l'efficacité de la structure NoSQL pour des requêtes fréquentes.

### 🔄 Test de Réplication (`test_replication.py`)
* **État du Cluster** : Détection automatique du nœud `PRIMARY`.
* **Vérification** : Test de "Write Propagation" réussi (la donnée écrite sur le maître est immédiatement disponible en lecture).

---

## 🚀 Installation et Utilisation

### Pré-requis
* Python 3.12+
* MongoDB configuré en mode Replica Set (`--replSet rs0`)
* Fichier `.env` configuré avec vos accès AWS S3.

### Exécution
1. Préparation de l'environnement

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2. Lancement du Pipeline ETL
```
python3 transform_and_load.py
```
3. Exécution des Audits de Qualité
```
python3 audit_integrity.py
python3 temp_access.py
```
  
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