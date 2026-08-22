"""Exploration Gaussian Mixture Model (GMM) sur `marts.mart_clustering_dataset`
-- alternative à K-Means (`kmeans_model.py`) testée sur proposition de
l'utilisateur le 2026-08-22, après le constat (K-Means + DBSCAN) que les
profils de communes se distinguent par des dégradés continus plutôt que par
des groupes franchement séparés (silhouette ~0,28-0,33 des deux côtés,
DBSCAN incapable de trouver une frontière de densité nette).

Contrairement à K-Means (frontières dures, clusters implicitement
sphériques et de taille comparable), GMM modélise chaque cluster comme une
distribution gaussienne et donne, pour chaque commune, une PROBABILITÉ
d'appartenance à chaque composante plutôt qu'une classe unique --
potentiellement plus honnête ici vu l'absence de séparation nette déjà
constatée. `n_components` reste borné à {3, 4} (même contrainte produit que
K-Means : 3-4 profils maximum pour rester actionnable côté
gestionnaire/collectivité). Plusieurs formes de covariance sont comparées :
`spherical` se rapproche le plus de K-Means (clusters ronds, même variance
dans toutes les directions), `full` est la plus flexible (chaque cluster a
sa propre forme elliptique orientée librement), `diag`/`tied` sont des
compromis intermédiaires.

Comme pour `dbscan_explore.py`, ce module est exploratoire et séparé du
pipeline de production (`kmeans_model.py`/`cli.py`) -- il ne remplace rien
tant que le résultat ne s'avère pas clairement meilleur.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass

import mlflow
import numpy as np
from dotenv import load_dotenv
from sklearn.metrics import silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from ..loading import postgres_loader
from . import kmeans_model

logger = logging.getLogger(__name__)

DEFAULT_N_COMPONENTS: tuple[int, ...] = (3, 4)
DEFAULT_COVARIANCE_TYPES: tuple[str, ...] = ("full", "tied", "diag", "spherical")
DEFAULT_EXPERIMENT_NAME = "ve_clustering_gmm"


@dataclass
class GmmCandidateResult:
    n_components: int
    covariance_type: str
    bic: float
    aic: float
    silhouette: float
    # Confiance moyenne de l'assignation : moyenne, sur toutes les communes,
    # de la probabilité de la composante la plus probable. Proche de 1 =
    # assignations tranchées ; proche de 1/n_components = très flou (chaque
    # commune est presque également probable dans plusieurs clusters).
    mean_max_proba: float


def explore_gmm(
    df,
    n_components_values: tuple[int, ...] = DEFAULT_N_COMPONENTS,
    covariance_types: tuple[str, ...] = DEFAULT_COVARIANCE_TYPES,
    random_state: int = 42,
) -> list[GmmCandidateResult]:
    """Entraîne un GMM par combinaison (n_components, covariance_type) sur
    les features standardisées (mêmes `FEATURE_COLUMNS` que K-Means, pour
    comparaison directe) et retourne BIC/AIC/silhouette/confiance de
    chacune."""
    X = df[kmeans_model.FEATURE_COLUMNS]
    X_scaled = StandardScaler().fit_transform(X)

    results: list[GmmCandidateResult] = []
    for n_components in n_components_values:
        for covariance_type in covariance_types:
            gmm = GaussianMixture(
                n_components=n_components,
                covariance_type=covariance_type,
                random_state=random_state,
                n_init=5,
            )
            gmm.fit(X_scaled)
            labels = gmm.predict(X_scaled)
            proba = gmm.predict_proba(X_scaled)

            results.append(
                GmmCandidateResult(
                    n_components=n_components,
                    covariance_type=covariance_type,
                    bic=float(gmm.bic(X_scaled)),
                    aic=float(gmm.aic(X_scaled)),
                    silhouette=float(silhouette_score(X_scaled, labels)),
                    mean_max_proba=float(np.mean(proba.max(axis=1))),
                )
            )
    return results


def select_best_candidate(candidates: list[GmmCandidateResult]) -> GmmCandidateResult:
    """Le meilleur candidat au sens du BIC (plus bas = meilleur compromis
    qualité d'ajustement / complexité) -- critère natif de sélection de
    modèle pour les mélanges gaussiens, contrairement à K-Means qui n'a pas
    d'équivalent direct (d'où le recours à la silhouette pour ce dernier)."""
    return min(candidates, key=lambda c: c.bic)


def log_candidates_to_mlflow(
    candidates: list[GmmCandidateResult],
    n_communes: int,
    experiment_name: str = DEFAULT_EXPERIMENT_NAME,
    tracking_uri: str | None = None,
) -> None:
    resolved_tracking_uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI") or kmeans_model.DEFAULT_TRACKING_URI
    mlflow.set_tracking_uri(resolved_tracking_uri)
    mlflow.set_experiment(experiment_name)

    best = select_best_candidate(candidates)
    for c in candidates:
        with mlflow.start_run(run_name=f"n={c.n_components}_{c.covariance_type}"):
            mlflow.log_param("algorithm", "gmm")
            mlflow.log_param("n_components", c.n_components)
            mlflow.log_param("covariance_type", c.covariance_type)
            mlflow.log_param("features", kmeans_model.FEATURE_COLUMNS)
            mlflow.log_param("n_communes", n_communes)
            mlflow.log_param("selected", c is best)
            mlflow.log_metric("bic", c.bic)
            mlflow.log_metric("aic", c.aic)
            mlflow.log_metric("silhouette", c.silhouette)
            mlflow.log_metric("mean_max_proba", c.mean_max_proba)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Exploration GMM sur mart_clustering_dataset (comparaison face à K-Means, pas le pipeline retenu)"
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--source-table", default=kmeans_model.SOURCE_TABLE)
    parser.add_argument(
        "--n-components",
        default=",".join(str(n) for n in DEFAULT_N_COMPONENTS),
        help="Valeurs de n_components à tester, séparées par des virgules",
    )
    parser.add_argument(
        "--covariance-types",
        default=",".join(DEFAULT_COVARIANCE_TYPES),
        help="Types de covariance à tester, séparés par des virgules (full,tied,diag,spherical)",
    )
    parser.add_argument("--experiment-name", default=DEFAULT_EXPERIMENT_NAME)
    parser.add_argument("--tracking-uri", default=None)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    n_components_values = tuple(int(n.strip()) for n in args.n_components.split(","))
    covariance_types = tuple(c.strip() for c in args.covariance_types.split(","))

    engine = postgres_loader.get_engine(args.database_url)
    df = kmeans_model.load_clustering_dataset(engine, table=args.source_table)
    logger.info("%d communes chargées depuis '%s'", len(df), args.source_table)

    candidates = explore_gmm(df, n_components_values, covariance_types, random_state=args.random_state)
    log_candidates_to_mlflow(candidates, len(df), experiment_name=args.experiment_name, tracking_uri=args.tracking_uri)
    best = select_best_candidate(candidates)

    print(f"{len(df)} communes -- {len(candidates)} combinaisons (n_components x covariance_type) testées\n")
    print(f"{'n_components':>12} {'covariance':>11} {'bic':>14} {'aic':>14} {'silhouette':>11} {'confiance_moy':>14}")
    for c in candidates:
        marker = " *" if c is best else ""
        print(
            f"{c.n_components:>12} {c.covariance_type:>11} {c.bic:>14.1f} {c.aic:>14.1f} "
            f"{c.silhouette:>11.4f} {c.mean_max_proba:>14.4f}{marker}"
        )
    print(f"\n(*) meilleur au sens du BIC : n_components={best.n_components}, covariance_type='{best.covariance_type}'")
    print(f"Résultats tracés dans MLflow (expérience '{args.experiment_name}')")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
