"""Récupère les centroïdes (latitude/longitude) de toutes les communes
françaises via l'API publique geo.api.gouv.fr, en un seul appel réseau, et
écrit dbt/seeds/ref_centroides_communes.csv.

A exécuter directement sur ta machine (pas dans l'environnement Claude) :
la réponse complète (~35 000 communes) est trop volumineuse pour être
récupérée via l'outil de fetch web de Claude (limité à ~80 Ko par requête),
mais ne pose aucun problème pour une requête HTTP normale depuis ton Mac.

Usage (depuis la racine du projet) :
    python3 fetch_centroides.py

Ne nécessite aucune dépendance externe (urllib de la bibliothèque standard).
Ce script est un utilitaire ponctuel -- tu peux le supprimer une fois le
seed généré.
"""
from __future__ import annotations

import csv
import json
import urllib.request
from pathlib import Path

URL = "https://geo.api.gouv.fr/communes?fields=centre&format=json&geometry=centre"
OUTPUT_PATH = Path(__file__).resolve().parent / "dbt" / "seeds" / "ref_centroides_communes.csv"


def main() -> None:
    print(f"Téléchargement depuis {URL} ...")
    with urllib.request.urlopen(URL, timeout=60) as response:
        data = json.load(response)
    print(f"{len(data)} communes reçues.")

    output_path = OUTPUT_PATH
    if not output_path.parent.exists():
        output_path = Path(__file__).resolve().parent / "ref_centroides_communes.csv"
        print(f"Attention : dbt/seeds/ introuvable à côté du script, écriture dans {output_path}")

    n_written = 0
    n_skipped = 0
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["code_commune", "latitude", "longitude"])
        for commune in data:
            centre = commune.get("centre")
            code = commune.get("code")
            if not centre or not code:
                n_skipped += 1
                continue
            lon, lat = centre["coordinates"]
            writer.writerow([code, lat, lon])
            n_written += 1

    print(f"Écrit {n_written} lignes dans {output_path} ({n_skipped} communes sans centre connu, ignorées).")


if __name__ == "__main__":
    main()
