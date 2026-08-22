"""Tests de bon fonctionnement du clustering K-Means (ve_pipeline.clustering).

Utilise SQLite (fichier, via tmp_path) comme stand-in de Postgres pour
`marts.mart_clustering_dataset` -- même code SQLAlchemy des deux côtés,
comme pour `tests/test_postgres_loader.py`. Le tracking MLflow pointe vers une base SQLite temporaire
(`sqlite:///<tmp_path>/mlflow.db`) -- MLflow >=3 refuse le backend fichier
brut par défaut (voir DEFAULT_TRACKING_URI dans kmeans_model.py) -- jamais
le `mlflow.db` du dépôt, pour ne rien polluer entre les runs de tests.

Couvre :
  1. le chargement des features depuis Postgres/SQLite (cast du booléen,
     détection de NULL inattendus),
  2. la sélection de k par score de silhouette sur un jeu de données
     synthétique où 3 groupes bien séparés existent réellement,
  3. l'entraînement + tracking MLflow (params/métriques/modèle loggés),
  4. l'écriture des assignations (comptage, idempotence d'un ré-entraînement).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ve_pipeline.clustering import kmeans_model
from ve_pipeline.loading import postgres_loader


def _sqlite_engine(tmp_path):
    return postgres_loader.get_engine(f"sqlite:///{tmp_path}/staging.db")


def _make_synthetic_dataset(n_per_group: int = 40, seed: int = 0) -> pd.DataFrame:
    """3 groupes de communes nettement séparés sur les 6 features, pour que
    le score de silhouette distingue clairement k=3 comme le meilleur choix
    face à k=4 (qui devrait re-découper un groupe déjà homogène)."""
    rng = np.random.default_rng(seed)

    def group(center: dict[str, float], n: int) -> pd.DataFrame:
        # demarrage_ve_tardif traité comme les autres features (bruit faible
        # autour d'une valeur par groupe) plutôt qu'un booléen tiré au hasard
        # indépendamment du groupe : sinon ce bruit "intra-groupe" non corrélé
        # peut, une fois standardisé, rivaliser avec le vrai signal entre
        # groupes et pousser k=4 à re-découper un groupe homogène sur ce seul
        # axe -- pas représentatif de "3 groupes réellement séparés".
        return pd.DataFrame(
            {col: rng.normal(loc=center[col], scale=0.05, size=n) for col in kmeans_model.FEATURE_COLUMNS}
        )

    centers = [
        {
            "part_thermosensible": 10.0,
            "taux_chauffage_electrique": 15.0,
            "croissance_immat_ve_pct": 0.2,
            "demarrage_ve_tardif": 0.0,
            "taux_couverture_afir": 2.5,
            "ratio_pdc_par_100_ve": 2.5,
        },
        {
            "part_thermosensible": 30.0,
            "taux_chauffage_electrique": 45.0,
            "croissance_immat_ve_pct": 1.0,
            "demarrage_ve_tardif": 1.0,
            "taux_couverture_afir": 0.8,
            "ratio_pdc_par_100_ve": 0.8,
        },
        {
            "part_thermosensible": 50.0,
            "taux_chauffage_electrique": 70.0,
            "croissance_immat_ve_pct": 3.0,
            "demarrage_ve_tardif": 0.0,
            "taux_couverture_afir": 0.2,
            "ratio_pdc_par_100_ve": 0.2,
        },
    ]
    groups = [group(c, n_per_group) for c in centers]
    df = pd.concat(groups, ignore_index=True)
    df.insert(0, "code_commune", [f"{i:05d}" for i in range(len(df))])
    return df


def test_load_clustering_dataset_casts_boolean_and_reads_features(tmp_path):
    engine = _sqlite_engine(tmp_path)
    df = _make_synthetic_dataset(n_per_group=2)
    df.to_sql("mart_clustering_dataset", engine, index=False)

    loaded = kmeans_model.load_clustering_dataset(engine, table="mart_clustering_dataset")

    assert list(loaded.columns) == ["code_commune", *kmeans_model.FEATURE_COLUMNS]
    assert loaded["demarrage_ve_tardif"].dtype == int
    assert len(loaded) == 6


def test_load_clustering_dataset_rejects_unexpected_nulls(tmp_path):
    engine = _sqlite_engine(tmp_path)
    df = _make_synthetic_dataset(n_per_group=2)
    df.loc[0, "taux_couverture_afir"] = None
    df.to_sql("mart_clustering_dataset", engine, index=False)

    with pytest.raises(ValueError, match="NULL inattendues"):
        kmeans_model.load_clustering_dataset(engine, table="mart_clustering_dataset")


def test_load_clustering_dataset_rejects_empty_table(tmp_path):
    engine = _sqlite_engine(tmp_path)
    df = _make_synthetic_dataset(n_per_group=1).iloc[0:0]
    df.to_sql("mart_clustering_dataset", engine, index=False)

    with pytest.raises(ValueError, match="aucune ligne"):
        kmeans_model.load_clustering_dataset(engine, table="mart_clustering_dataset")


def test_evaluate_k_candidates_prefers_k_matching_real_group_count():
    df = _make_synthetic_dataset(n_per_group=40)

    candidates = kmeans_model.evaluate_k_candidates(df, k_values=(3, 4))

    by_k = {c.k: c for c in candidates}
    assert set(by_k) == {3, 4}
    # 3 groupes réellement séparés dans les données synthétiques : k=3 doit
    # obtenir un meilleur score de silhouette que k=4 (qui re-découpe un
    # groupe homogène en 2).
    assert by_k[3].silhouette > by_k[4].silhouette
    assert kmeans_model.select_best_k(candidates) == 3


def test_train_and_log_tracks_run_and_returns_assignments_for_every_commune(tmp_path, monkeypatch):
    # mlflow écrit les artefacts (modèle, profil CSV) sous "./mlruns" relatif
    # au CWD même avec un backend de tracking SQLite (le backend store et la
    # racine des artefacts sont deux choses distinctes) -- se placer dans
    # tmp_path pour ne rien laisser dans le dépôt réel (constaté le 2026-08-22).
    monkeypatch.chdir(tmp_path)
    df = _make_synthetic_dataset(n_per_group=40)
    tracking_uri = f"sqlite:///{tmp_path}/mlflow.db"

    result = kmeans_model.train_and_log(
        df,
        k_candidates=(3, 4),
        experiment_name="test_ve_clustering",
        tracking_uri=tracking_uri,
    )

    assert result.k == 3
    assert result.run_id
    assert len(result.candidates) == 2
    assert len(result.assignments) == len(df)
    assert set(result.assignments["cluster_id"].unique()) == {0, 1, 2}
    assert set(result.assignments.columns) == {"code_commune", "cluster_id", *kmeans_model.FEATURE_COLUMNS}


def test_train_and_log_respects_explicit_k_override(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # voir commentaire du test précédent
    df = _make_synthetic_dataset(n_per_group=40)
    tracking_uri = f"sqlite:///{tmp_path}/mlflow.db"

    result = kmeans_model.train_and_log(
        df,
        k=4,
        k_candidates=(3, 4),
        experiment_name="test_ve_clustering_override",
        tracking_uri=tracking_uri,
    )

    assert result.k == 4
    assert set(result.assignments["cluster_id"].unique()) == {0, 1, 2, 3}


def test_write_cluster_assignments_creates_table_with_correct_row_count_and_is_idempotent(tmp_path):
    engine = _sqlite_engine(tmp_path)
    df = _make_synthetic_dataset(n_per_group=10)
    assignments = df[["code_commune", *kmeans_model.FEATURE_COLUMNS]].copy()
    assignments["cluster_id"] = [i % 3 for i in range(len(df))]

    n1 = kmeans_model.write_cluster_assignments(engine, assignments, run_id="run-1", table="ml__test_assignments")
    assert n1 == len(df)

    # Ré-entraînement (nouveau run_id) : la table est remplacée, pas dupliquée.
    n2 = kmeans_model.write_cluster_assignments(engine, assignments, run_id="run-2", table="ml__test_assignments")
    assert n2 == len(df)

    stored = pd.read_sql_table("ml__test_assignments", engine)
    assert len(stored) == len(df)
    assert (stored["mlflow_run_id"] == "run-2").all()
    assert "trained_at" in stored.columns
