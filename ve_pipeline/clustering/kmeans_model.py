"""Entraînement du clustering K-Means sur `marts.mart_clustering_dataset`.

Ce module lit le dataset préparé par dbt
(`dbt/models/marts/mart_clustering_dataset.sql`, 32 101 communes en
conditions réelles au 2026-08-21) depuis Postgres/Neon, standardise les
features retenues, choisit k parmi {3, 4} (décision produit : 3-4 profils
maximum pour rester actionnable côté gestionnaire de bornes/collectivité,
cf. mémoire projet -- pas un choix entièrement data-driven sans contrainte)
via le score de silhouette, entraîne le pipeline final et trace toute
l'expérimentation dans MLflow (paramètres, métriques, artefact modèle,
profil moyen par cluster).

Features retenues (décision du 2026-08-22, discussion avec l'utilisateur) :
  - `part_thermosensible`, `taux_chauffage_electrique` : risque de tension
    sur le réseau électrique (sensibilité aux pics, chauffage électrique).
  - `croissance_immat_ve_pct`, `demarrage_ve_tardif` : dynamique d'adoption
    VE. Les deux sont gardées (pas redondantes) : pour les communes à
    démarrage tardif, `croissance_immat_ve_pct` est déjà une médiane
    imputée (valeur neutre) -- sans le flag, ces communes seraient
    indiscernables d'une commune à croissance réelle moyenne.
  - `taux_couverture_afir`, `ratio_pdc_par_100_ve` : équipement en bornes de
    recharge relatif au besoin (seuil réglementaire AFIR / moyenne
    nationale Insee par zone de densité).
`code_commune` sert de clé, pas de feature. `zone_densite` n'est
volontairement pas incluse : elle est déjà absorbée dans
`ratio_pdc_par_100_ve` (cf. commentaire dans mart_clustering_dataset.sql) --
à rejoindre après coup si besoin d'interprétation post-hoc des clusters
(pas fait par ce module).

Choix technique : `Pipeline` sklearn (StandardScaler + KMeans) plutôt que
standardiser à la main -- le même pipeline est loggé tel quel dans MLflow
(un seul artefact modèle, directement réutilisable pour scorer de nouvelles
communes sans reconstruire le scaler séparément).
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Features validées avec l'utilisateur le 2026-08-22 (voir docstring module).
# L'ordre est fixe : c'est aussi l'ordre des colonnes passées au pipeline
# sklearn, donc l'ordre attendu par le modèle une fois rechargé depuis MLflow.
FEATURE_COLUMNS: list[str] = [
    "part_thermosensible",
    "taux_chauffage_electrique",
    "croissance_immat_ve_pct",
    "demarrage_ve_tardif",
    "taux_couverture_afir",
    "ratio_pdc_par_100_ve",
]

SOURCE_TABLE = "marts.mart_clustering_dataset"
# Même convention que raw__<source>__<file> (postgres_loader.py) : pas de
# schéma SQL séparé, pour rester compatible SQLite en test comme Postgres
# en prod avec le même code.
DEFAULT_OUTPUT_TABLE = "ml__cluster_assignments"
DEFAULT_K_CANDIDATES: tuple[int, ...] = (3, 4)
# MLflow >=3 refuse par défaut le backend fichier brut ("file:./mlruns",
# géré par FileStore) -- il est passé en "maintenance mode" et lève une
# MlflowException à moins de positionner MLFLOW_ALLOW_FILE_STORE=true.
# Découvert en testant ce module (2026-08-22) avec mlflow 3.15.1. Plutôt que
# de dépendre de cette variable d'environnement, le repli local par défaut
# utilise un backend SQLite (toujours supporté, aucune infra à installer),
# conforme à la recommandation actuelle de MLflow.
DEFAULT_TRACKING_URI = "sqlite:///mlflow.db"


@dataclass
class KCandidateResult:
    k: int
    inertia: float
    silhouette: float


@dataclass
class TrainingResult:
    k: int
    run_id: str
    inertia: float
    silhouette: float
    candidates: list[KCandidateResult]
    assignments: pd.DataFrame  # code_commune, features, cluster_id


def load_clustering_dataset(engine: Engine, table: str = SOURCE_TABLE) -> pd.DataFrame:
    """Charge `code_commune` + les features de clustering depuis Postgres.

    `demarrage_ve_tardif` est un booléen côté Postgres -- casté en 0/1 ici
    pour pouvoir le standardiser comme les autres features numériques.
    """
    columns = ", ".join(FEATURE_COLUMNS)
    query = f"SELECT code_commune, {columns} FROM {table}"  # noqa: S608 - table/colonnes fixes, pas d'entrée utilisateur
    df = pd.read_sql(text(query), engine)

    if df.empty:
        raise ValueError(f"'{table}' ne contient aucune ligne -- avez-vous lancé `dbt run` ?")

    missing = df[FEATURE_COLUMNS].isna().sum()
    if missing.any():
        raise ValueError(
            f"Valeurs NULL inattendues dans '{table}' (le mart ne doit plus en produire après "
            f"imputation) : {missing[missing > 0].to_dict()}"
        )

    df["demarrage_ve_tardif"] = df["demarrage_ve_tardif"].astype(int)
    return df


def build_pipeline(k: int, random_state: int = 42) -> Pipeline:
    return Pipeline(
        [
            ("scaler", StandardScaler()),
            ("kmeans", KMeans(n_clusters=k, random_state=random_state, n_init=10)),
        ]
    )


def evaluate_k_candidates(
    df: pd.DataFrame,
    k_values: tuple[int, ...] = DEFAULT_K_CANDIDATES,
    random_state: int = 42,
) -> list[KCandidateResult]:
    """Entraîne un pipeline par valeur de k candidate et calcule inertie +
    score de silhouette, pour choisir k parmi {3, 4} (cf. docstring module)."""
    X = df[FEATURE_COLUMNS]
    results: list[KCandidateResult] = []
    for k in k_values:
        pipeline = build_pipeline(k, random_state=random_state)
        labels = pipeline.fit_predict(X)
        X_scaled = pipeline.named_steps["scaler"].transform(X)
        results.append(
            KCandidateResult(
                k=k,
                inertia=float(pipeline.named_steps["kmeans"].inertia_),
                silhouette=float(silhouette_score(X_scaled, labels)),
            )
        )
    return results


def select_best_k(candidates: list[KCandidateResult]) -> int:
    """Le k retenu est celui au meilleur score de silhouette (clusters les
    mieux séparés). En cas d'égalité, garde le plus petit k -- modèle plus
    simple à interpréter côté utilisateur final."""
    best = max(candidates, key=lambda c: (c.silhouette, -c.k))
    return best.k


def _cluster_profile(df: pd.DataFrame, labels) -> pd.DataFrame:
    """Moyenne de chaque feature par cluster + effectif -- sert à
    l'interprétation/au nommage des clusters (pas fait par ce module,
    restitué tel quel en artefact MLflow pour analyse a posteriori)."""
    profile = df[FEATURE_COLUMNS].copy()
    profile["cluster_id"] = labels
    profile["nb_communes"] = 1
    summary = profile.groupby("cluster_id").agg(
        {**{col: "mean" for col in FEATURE_COLUMNS}, "nb_communes": "sum"}
    )
    return summary.reset_index()


def train_and_log(
    df: pd.DataFrame,
    k: int | None = None,
    k_candidates: tuple[int, ...] = DEFAULT_K_CANDIDATES,
    experiment_name: str = "ve_clustering",
    tracking_uri: str | None = None,
    random_state: int = 42,
) -> TrainingResult:
    """Sélectionne k (si non fourni explicitement) parmi `k_candidates` par
    score de silhouette, entraîne le pipeline final et trace tout dans
    MLflow : un run par candidat testé (`selected=False`), plus un run final
    marqué `selected=True` portant l'artefact modèle à utiliser en aval.

    Résolution de l'URI de tracking, par ordre de priorité : `tracking_uri`
    explicite > variable d'environnement `MLFLOW_TRACKING_URI` > repli local
    SQLite (`DEFAULT_TRACKING_URI`, voir sa docstring)."""
    resolved_tracking_uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_TRACKING_URI
    mlflow.set_tracking_uri(resolved_tracking_uri)
    mlflow.set_experiment(experiment_name)

    candidates = evaluate_k_candidates(df, k_candidates, random_state=random_state)
    for c in candidates:
        with mlflow.start_run(run_name=f"k={c.k}_candidat"):
            mlflow.log_param("k", c.k)
            mlflow.log_param("features", FEATURE_COLUMNS)
            mlflow.log_param("selected", False)
            mlflow.log_metric("inertia", c.inertia)
            mlflow.log_metric("silhouette", c.silhouette)

    chosen_k = k if k is not None else select_best_k(candidates)
    logger.info("k retenu : %d (candidats testés : %s)", chosen_k, [c.k for c in candidates])

    X = df[FEATURE_COLUMNS]
    pipeline = build_pipeline(chosen_k, random_state=random_state)
    labels = pipeline.fit_predict(X)
    X_scaled = pipeline.named_steps["scaler"].transform(X)
    inertia = float(pipeline.named_steps["kmeans"].inertia_)
    silhouette = float(silhouette_score(X_scaled, labels))

    with mlflow.start_run(run_name=f"k={chosen_k}_final") as run:
        mlflow.log_param("k", chosen_k)
        mlflow.log_param("features", FEATURE_COLUMNS)
        mlflow.log_param("n_communes", len(df))
        mlflow.log_param("selected", True)
        mlflow.log_metric("inertia", inertia)
        mlflow.log_metric("silhouette", silhouette)
        # `name=` plutôt que `artifact_path=` : mlflow >=3 déprécie ce
        # dernier (avertissement observé en testant ce module le 2026-08-22
        # avec mlflow 3.15.1).
        mlflow.sklearn.log_model(pipeline, name="model")

        profile = _cluster_profile(df, labels)
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile_path = Path(tmp_dir) / "cluster_profile.csv"
            profile.to_csv(profile_path, index=False)
            mlflow.log_artifact(str(profile_path))

        run_id = run.info.run_id

    assignments = df[["code_commune", *FEATURE_COLUMNS]].copy()
    assignments["cluster_id"] = labels

    return TrainingResult(
        k=chosen_k,
        run_id=run_id,
        inertia=inertia,
        silhouette=silhouette,
        candidates=candidates,
        assignments=assignments,
    )


def write_cluster_assignments(
    engine: Engine,
    assignments: pd.DataFrame,
    run_id: str,
    table: str = DEFAULT_OUTPUT_TABLE,
) -> int:
    """Écrit `code_commune` + features + `cluster_id` dans une table à plat.

    `if_exists='replace'` : la table représente le DERNIER clustering
    entraîné, pas un historique -- cohérent avec la convention "overwrite"
    déjà utilisée partout ailleurs dans ce projet (partitions S3 `dt=`,
    tables `raw__*`). L'historique complet (tous les k testés, métriques,
    modèle) reste dans MLflow, pas dupliqué ici.
    """
    out = assignments.copy()
    out["mlflow_run_id"] = run_id
    out["trained_at"] = datetime.now(timezone.utc).isoformat()

    out.to_sql(table, engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        loaded_count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one()
    if loaded_count != len(out):
        raise RuntimeError(f"Table '{table}' : {loaded_count} lignes écrites, {len(out)} attendues.")
    return loaded_count
