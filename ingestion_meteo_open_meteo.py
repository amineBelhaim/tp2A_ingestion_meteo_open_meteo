"""TP 2A - Ingestion API meteo (Open-Meteo).

Pipeline simple en deux temps :
  1. extraire_meteo_brut   : appelle l'API Open-Meteo pour plusieurs villes
                             et stocke la reponse JSON BRUTE dans un fichier.
  2. transformer_meteo     : lit le brut, ne garde que les champs utiles
                             et produit une structure exploitable (1 ligne / ville).

Choix d'architecture (conforme au cours) :
  - extraction et transformation sont SEPAREES (deux taches distinctes) ;
  - la donnee brute est ecrite dans un fichier ; XCom ne transporte que le
    CHEMIN du fichier, pas la donnee complete ;
  - on distingue clairement ce qui vient de l'API (brut) de ce qui est
    prepare pour le pipeline (structure aplatie, champs selectionnes).
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# --- Configuration metier : villes a ingerer (au moins 3) -------------------
VILLES = [
    {"nom": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"nom": "Berlin", "latitude": 52.5200, "longitude": 13.4050},
    {"nom": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


@dag(
    dag_id="ingestion_meteo_open_meteo",
    description="TP 2A - Ingestion API meteo Open-Meteo pour plusieurs villes",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tp", "meteo", "api"],
)
def ingestion_meteo():

    @task
    def extraire_meteo_brut() -> str:
        """Appelle l'API pour chaque ville et stocke la reponse JSON BRUTE.

        Aucune transformation ici : on conserve la donnee telle que l'API la
        renvoie. On ecrit le brut dans un fichier et on transmet seulement le
        chemin via XCom.
        """
        reponses_brutes = []
        for ville in VILLES:
            params = {
                "latitude": ville["latitude"],
                "longitude": ville["longitude"],
                "current_weather": True,
            }
            reponse = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            reponse.raise_for_status()
            reponses_brutes.append(
                {"ville": ville["nom"], "reponse_api": reponse.json()}
            )
            logger.info("Meteo brute recuperee pour %s", ville["nom"])

        chemin_brut = Path(tempfile.gettempdir()) / "meteo_brut.json"
        chemin_brut.write_text(
            json.dumps(reponses_brutes, ensure_ascii=False, indent=2)
        )
        logger.info(
            "JSON brut ecrit : %s (%s villes)", chemin_brut, len(reponses_brutes)
        )
        return str(chemin_brut)

    @task
    def transformer_meteo(chemin_brut: str) -> str:
        """Lit le brut et prepare une structure exploitable pour la table cible.

        On ne garde QUE les champs utiles au besoin metier et on aplatit la
        reponse en une ligne par ville (coherent avec une future table SQL).
        """
        reponses_brutes = json.loads(Path(chemin_brut).read_text())

        lignes_preparees = []
        for element in reponses_brutes:
            reponse = element["reponse_api"]
            meteo = reponse["current_weather"]

            ligne = {
                "ville": element["ville"],
                "latitude": reponse["latitude"],
                "longitude": reponse["longitude"],
                "horodatage_mesure": meteo["time"],
                "temperature_c": meteo["temperature"],
                "vent_kmh": meteo["windspeed"],
                "code_meteo": meteo["weathercode"],
            }
            lignes_preparees.append(ligne)
            logger.info("Ligne preparee : %s", ligne)

        chemin_propre = Path(tempfile.gettempdir()) / "meteo_prepare.json"
        chemin_propre.write_text(
            json.dumps(lignes_preparees, ensure_ascii=False, indent=2)
        )
        logger.info(
            "Donnees preparees : %s lignes -> %s",
            len(lignes_preparees),
            chemin_propre,
        )
        return str(chemin_propre)

    # Dependance utile : la transformation depend de l'extraction
    chemin_brut = extraire_meteo_brut()
    transformer_meteo(chemin_brut)


ingestion_meteo()
