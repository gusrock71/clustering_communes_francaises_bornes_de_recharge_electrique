"""CLI de vérification de fraîcheur du filtre serveur Enedis.

Exemple :
    python -m ve_pipeline.ingestion.freshness_cli

Codes de sortie :
    0 -- filtre à jour (aucune année plus récente publiée par Enedis)
    1 -- une année plus récente est disponible : mettre à jour `qs=annee:...`
         et `value_filter` dans `ve_pipeline/ingestion/config.py`, puis
         relancer l'ingestion Enedis
    2 -- vérification impossible (page catalogue injoignable ou format non
         reconnu) -- distinct de 1, pour ne pas déclencher une fausse alerte
         "nouvelle donnée disponible" en cas de simple souci réseau

Pensé pour tourner seul (cron, tâche planifiée, action manuelle avant un
run d'ingestion) sans dépendre d'un orchestrateur -- cohérent avec la
décision J4 du projet de ne pas introduire Airflow pour ce MVP.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from dotenv import load_dotenv

from .freshness import DEFAULT_ENEDIS_DATASET_PAGE_URL, EnedisFreshnessCheckError, check_enedis_freshness


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Vérifie si le filtre Enedis (config.py) est à jour")
    parser.add_argument("--url", default=DEFAULT_ENEDIS_DATASET_PAGE_URL, help="Page catalogue Enedis à consulter")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    try:
        report = check_enedis_freshness(url=args.url, timeout=args.timeout)
    except EnedisFreshnessCheckError as exc:
        print(json.dumps({"error": str(exc)}, indent=2, ensure_ascii=False))
        print(f"\nVérification impossible : {exc}", file=sys.stderr)
        return 2

    print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))

    if report.is_stale:
        print(
            f"\nNouvelle année disponible côté Enedis ({report.latest_covered_year}) : "
            f"le filtre est encore figé sur {report.configured_year} dans config.py. "
            "Mettre à jour 'qs=annee:...' et value_filter, puis relancer l'ingestion Enedis.",
            file=sys.stderr,
        )
        return 1

    print(f"\nFiltre à jour (année {report.configured_year}, dernière année Enedis disponible : "
          f"{report.latest_covered_year}).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
