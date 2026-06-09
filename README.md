# TP 2A — Ingestion API météo (Open-Meteo)

Pipeline Airflow qui récupère la météo courante de plusieurs villes depuis l'API publique **Open-Meteo**, puis prépare une structure de données exploitable pour une future table cible. L'objectif est de **séparer proprement la récupération (extraction) de la transformation**, et de ne conserver que les champs réellement utiles.

## Stack
- Apache Airflow 3.2.2 (TaskFlow API : `@dag` / `@task`)
- Python 3.14
- API : Open-Meteo (publique, gratuite, sans clé)
- Exécution en local via `airflow standalone` (WSL / Ubuntu)

## Le DAG
Fichier : [`ingestion_meteo_open_meteo.py`](ingestion_meteo_open_meteo.py) — DAG `ingestion_meteo_open_meteo`, déclenché **manuellement** (`schedule=None`), composé de **2 tâches** :

```
extraire_meteo_brut  -->  transformer_meteo
```

| Tâche | Rôle |
|-------|------|
| `extraire_meteo_brut` | Appelle l'API Open-Meteo pour chaque ville (Paris, Berlin, Madrid) et stocke la **réponse JSON brute** dans un fichier. Aucune transformation. |
| `transformer_meteo` | Lit le fichier brut, sélectionne les champs utiles et produit une **structure aplatie (1 ligne par ville)** prête pour une table cible. |

## Séparation API brute / donnée préparée
Conformément au cours, on distingue clairement ce qui vient de l'API de ce qui est préparé pour le pipeline :

- la **réponse brute** complète de l'API est conservée dans `meteo_brut.json` (donnée source, non modifiée) ;
- la **donnée préparée** (champs sélectionnés, aplatie) est écrite dans `meteo_prepare.json` ;
- **XCom ne transporte que le chemin** de ces fichiers, jamais la donnée complète (bonne pratique : transmettre une référence, pas le volume de données).

## Champs retenus (et pourquoi)
La consigne est de **ne pas tout garder sans justification**. Champs conservés :

| Champ préparé | Source API | Pourquoi le garder |
|---------------|-----------|--------------------|
| `ville` | (paramètre métier) | Identifie la mesure ; clé métier indispensable. |
| `latitude` / `longitude` | `latitude` / `longitude` | Localisent le point de mesure ; cohérence géographique de la table cible. |
| `horodatage_mesure` | `current_weather.time` | Date/heure de la mesure ; nécessaire pour historiser et éviter les doublons. |
| `temperature_c` | `current_weather.temperature` | Donnée météo centrale du besoin. |
| `vent_kmh` | `current_weather.windspeed` | Indicateur météo utile à l'analyse. |
| `code_meteo` | `current_weather.weathercode` | Qualifie l'état du ciel (soleil, pluie…). |

**Champs volontairement écartés** : `winddirection`, `is_day`, `elevation`, `generationtime_ms`, `utc_offset_seconds`, `timezone`, `timezone_abbreviation`. Non nécessaires au besoin actuel : on évite de polluer la future table.

## Cohérence avec la future table cible
La structure préparée correspond à une ligne de table SQL du type :

```sql
CREATE TABLE meteo_villes (
    ville              TEXT,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    horodatage_mesure  TIMESTAMP,
    temperature_c      DOUBLE PRECISION,
    vent_kmh           DOUBLE PRECISION,
    code_meteo         INTEGER
);
```

## Aperçu des données préparées
Résultat du run (voir aussi [`apercu_donnees_preparees.json`](apercu_donnees_preparees.json)) :

| ville | latitude | longitude | horodatage_mesure | temperature_c | vent_kmh | code_meteo |
|-------|----------|-----------|-------------------|---------------|----------|------------|
| Paris | 48.86 | 2.36 | 2026-06-09T08:30 | 14.9 | 14.1 | 3 |
| Berlin | 52.52 | 13.40 | 2026-06-09T08:30 | 16.7 | 15.8 | 3 |
| Madrid | 40.44 | -3.69 | 2026-06-09T08:30 | 22.1 | 13.2 | 0 |

## Comment l'exécuter
```bash
export AIRFLOW_HOME=~/airflow-tp
source venv/bin/activate
airflow standalone
# Interface : http://localhost:8080 (login : admin)
# Activer le DAG (toggle) puis "Déclencher"
```

## Preuve d'exécution
Les 2 tâches en **Succès** :

![Exécution réussie](execution.png)

Logs de `transformer_meteo` (aperçu des lignes préparées) :

![Logs transformation](logs_transformation.png)
