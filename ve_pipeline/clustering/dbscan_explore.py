"""Exploration DBSCAN sur `marts.mart_clustering_dataset` -- comparaison
exploratoire face à K-Means (`ve_pipeline.clustering.kmeans_model`), PAS un
remplacement retenu par défaut (discussion du 2026-08-22).

Motif de prudence documenté avec l'utilisateur avant même de coder ceci :
DBSCAN utilise un rayon de voisinage (`eps`) unique sur tout l'espace des
features, qui sont ici très hétérogènes en densité -- deux features
plafonnées à 3 (`taux_couverture_afir`, `ratio_pdc_par_100_ve`), une quasi
binaire (`demarrage_ve_tardif`), les autres continues. Risque concret :
fusionner en un seul cluster le dégradé continu qui, à k=4 en K-Means, se
scinde utilement sur l'axe risque réseau (thermosensibilité/chauffage
électrique), ou classer une grosse part des communes en "bruit" (label -1)
faute de densité suffisante.

Ce module ne remplace donc pas `kmeans_model.py` : il teste une grille de
combinaisons (eps, min_samples), calcule pour chacune le nombre de clusters,
la part de bruit et un score de silhouette (calculé en excluant le bruit),
et trace chaque combinaison dans une expérience MLflow séparée
(`ve_clustering_dbscan`) pour comparaison visuelle avec les runs K-Means.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass

import mlflow
from dotenv import load_dotenv
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from ..loading import postgres_loader
from . import kmeans_model

logger = logging.getLogger(__name__)

DEFAULT_EPS_VALUES: tuple[float, ...] = (0.5, 1.0, 1.5, 2.0, 3.0)
DEFAULT_MIN_SAMPLES_VALUES: tuple[int, ...] = (5, 10, 30, 50)
DEFAULT_EXPERIMENT_NAME = "ve_clustering_dbscan"


@dataclass
class DbscanCandidateResult:
    eps: float
    min_samples: int
    n_clusters: int
    n_noise: int
    noise_pct: float
    silhouette: float | None  # None si < 2 clusters exploitables hors bruit


def explore_dbscan(
    df,
    eps_values: tuple[float, ...] = DEFAULT_EPS_VALUES,
    min_samples_values: tuple[int, ...] = DEFAULT_MIN_SAMPLES_VALUES,
) -> list[DbscanCandidateResult]:
    """Teste chaque combinaison (eps, min_samples) sur les features
    standardisées (même `FEATURE_COLUMNS` que K-Means, pour comparaison
    directe) et retourne les métriques de chacune."""
    X = df[kmeans_model.FEATURE_COLUMNS]
    X_scaled = StandardScaler().fit_transform(X)

    results: list[DbscanCandidateResult] = []
    for eps in eps_values:
        for min_samples in min_samples_values:
            labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(X_scaled)
            n_noise = int((labels == -1).sum())
            n_clusters = len(set(labels) - {-1})

            # silhouette exige >= 2 clusters, calculée hors bruit (label -1
            # n'est pas un vrai cluster, l'y inclure fausserait le score).
            silhouette = None
            mask = labels != -1
            if n_clusters >= 2 and mask.sum() > n_clusters:
                silhouette = float(silhouette_score(X_scaled[mask], labels[mask]))

            results.append(
                DbscanCandidateResult(
                    eps=eps,
                    min_samples=min_samples,
                    n_clusters=n_clusters,
                    n_noise=n_noise,
                    noise_pct=round(100 * n_noise / len(df), 2),
                    silhouette=silhouette,
                )
            )
    return results


def log_candidates_to_mlflow(
    candidates: list[DbscanCandidateResult],
    n_communes: int,
    experiment_name: str = DEFAULT_EXPERIMENT_NAME,
    tracking_uri: str | None = None,
) -> None:
    resolved_tracking_uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI") or kmeans_model.DEFAULT_TRACKING_URI
    mlflow.set_tracking_uri(resolved_tracking_uri)
    mlflow.set_experiment(experiment_name)

    for c in candidates:
        with mlflow.start_run(run_name=f"eps={c.eps}_min_samples={c.min_samples}"):
            mlflow.log_param("algorithm", "dbscan")
            mlflow.log_param("eps", c.eps)
            mlflow.log_param("min_samples", c.min_samples)
            mlflow.log_param("features", kmeans_model.FEATURE_COLUMNS)
            mlflow.log_param("n_communes", n_communes)
            mlflow.log_metric("n_clusters", c.n_clusters)
            mlflow.log_metric("n_noise", c.n_noise)
            mlflow.log_metric("noise_pct", c.noise_pct)
            if c.silhouette is not None:
                mlflow.log_metric("silhouette", c.silhouette)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Exploration DBSCAN sur mart_clustering_dataset (comparaison face à K-Means, pas le pipeline retenu)"
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--source-table", default=kmeans_model.SOURCE_TABLE)
    parser.add_argument("--eps", default=",".join(str(e) for e in DEFAULT_EPS_VALUES), help="Valeurs de eps à tester, séparées par des virgules")
    parser.add_argument(
        "--min-samples",
        default=",".join(str(m) for m in DEFAULT_MIN_SAMPLES_VALUES),
        help="Valeurs de min_samples à tester, séparées par des virgules",
    )
    parser.add_argument("--experiment-name", default=DEFAULT_EXPERIMENT_NAME)
    parser.add_argument("--tracking-uri", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    eps_values = tuple(float(e.strip()) for e in args.eps.split(","))
    min_samples_values = tuple(int(m.strip()) for m in args.min_samples.split(","))

    engine = postgres_loader.get_engine(args.database_url)
    df = kmeans_model.load_clustering_dataset(engine, table=args.source_table)
    logger.info("%d communes chargées depuis '%s'", len(df), args.source_table)

    candidates = explore_dbscan(df, eps_values, min_samples_values)
    log_candidates_to_mlflow(candidates, len(df), experiment_name=args.experiment_name, tracking_uri=args.tracking_uri)

    print(f"{len(df)} communes -- {len(candidates)} combinaisons (eps x min_samples) testées\n")
    print(f"{'eps':>6} {'min_samples':>12} {'n_clusters':>11} {'n_noise':>8} {'noise_%':>8} {'silhouette':>11}")
    for c in candidates:
        sil = f"{c.silhouette:.4f}" if c.silhouette is not None else "n/a"
        print(f"{c.eps:>6} {c.min_samples:>12} {c.n_clusters:>11} {c.n_noise:>8} {c.noise_pct:>7.2f}% {sil:>11}")
    print(f"\nRésultats tracés dans MLflow (expérience '{args.experiment_name}')")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
