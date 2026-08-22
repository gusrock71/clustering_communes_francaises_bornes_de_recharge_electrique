"""CLI d'entraînement du clustering K-Means sur `marts.mart_clustering_dataset`.

Exemples :
    DATABASE_URL=postgresql+psycopg2://user:pass@host/db \\
        python -m ve_pipeline.clustering.cli

    # Forcer k=4 plutôt que de le sélectionner automatiquement par silhouette :
    python -m ve_pipeline.clustering.cli --k 4

    # Suivi MLflow sur un serveur distant plutôt qu'en local (./mlruns) :
    python -m ve_pipeline.clustering.cli --tracking-uri http://localhost:5000

Sans DATABASE_URL, utilise le même repli SQLite local que
`ve_pipeline.loading.cli` (`ve_pipeline_staging.db`) -- pratique pour
vérifier la mécanique sans base Postgres réelle, mais `marts.mart_clustering_dataset`
n'existe alors que si vous l'y avez chargé vous-même (ce n'est pas un cas
d'usage réel, seulement un filet de sécurité pour les tests).
"""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv

from ..loading import postgres_loader
from . import kmeans_model


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # charge .env à la racine du projet (DATABASE_URL, ...)
    parser = argparse.ArgumentParser(description="Entraînement K-Means sur mart_clustering_dataset")
    parser.add_argument("--database-url", default=None, help="DSN SQLAlchemy (sinon DATABASE_URL ou SQLite local)")
    parser.add_argument("--source-table", default=kmeans_model.SOURCE_TABLE)
    parser.add_argument(
        "--k",
        type=int,
        default=None,
        help="Force un k précis (sinon sélection automatique parmi --k-candidates par score de silhouette)",
    )
    parser.add_argument(
        "--k-candidates",
        default=",".join(str(k) for k in kmeans_model.DEFAULT_K_CANDIDATES),
        help="Valeurs de k à comparer, séparées par des virgules (ex: 3,4)",
    )
    parser.add_argument("--experiment-name", default="ve_clustering")
    parser.add_argument(
        "--tracking-uri",
        default=None,
        help="URI de tracking MLflow (sinon MLFLOW_TRACKING_URI ou ./mlruns local par défaut)",
    )
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument(
        "--no-write-back",
        action="store_true",
        help=f"Ne pas écrire les assignations dans '{kmeans_model.DEFAULT_OUTPUT_TABLE}' (juste entraîner/tracer)",
    )
    parser.add_argument("--output-table", default=kmeans_model.DEFAULT_OUTPUT_TABLE)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    k_candidates = tuple(int(k.strip()) for k in args.k_candidates.split(","))

    engine = postgres_loader.get_engine(args.database_url)
    df = kmeans_model.load_clustering_dataset(engine, table=args.source_table)
    logger.info("%d communes chargées depuis '%s'", len(df), args.source_table)

    result = kmeans_model.train_and_log(
        df,
        k=args.k,
        k_candidates=k_candidates,
        experiment_name=args.experiment_name,
        tracking_uri=args.tracking_uri,
        random_state=args.random_state,
    )

    print(f"k retenu : {result.k}")
    print(f"run MLflow : {result.run_id} (expérience '{args.experiment_name}')")
    print(f"silhouette : {result.silhouette:.4f} | inertie : {result.inertia:.2f}")
    print("candidats testés : " + ", ".join(f"k={c.k} (silhouette={c.silhouette:.4f})" for c in result.candidates))

    if not args.no_write_back:
        n = kmeans_model.write_cluster_assignments(engine, result.assignments, result.run_id, table=args.output_table)
        print(f"{n} assignations écrites dans '{args.output_table}'")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
