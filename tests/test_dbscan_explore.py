"""Tests de bon fonctionnement de l'exploration DBSCAN (exploratoire, pas le
pipeline de production -- voir tests/test_kmeans_model.py pour K-Means).

Réutilise le même générateur de dataset synthétique à 3 groupes séparés que
test_kmeans_model.py, pour vérifier que la mécanique (grille eps/min_samples,
calcul du bruit, silhouette hors bruit) fonctionne correctement, sans
prétendre que DBSCAN retrouve forcément les 3 "vrais" groupes (ce n'est pas
l'objectif de ces tests, juste la robustesse du code d'exploration).
"""

from __future__ import annotations

from ve_pipeline.clustering import dbscan_explore
from .test_kmeans_model import _make_synthetic_dataset


def test_explore_dbscan_returns_one_result_per_combination():
    df = _make_synthetic_dataset(n_per_group=30)

    results = dbscan_explore.explore_dbscan(df, eps_values=(0.5, 1.5), min_samples_values=(5, 10))

    assert len(results) == 4  # 2 eps x 2 min_samples
    combos = {(r.eps, r.min_samples) for r in results}
    assert combos == {(0.5, 5), (0.5, 10), (1.5, 5), (1.5, 10)}


def test_explore_dbscan_tiny_eps_produces_mostly_noise():
    df = _make_synthetic_dataset(n_per_group=30)

    # Un rayon quasiment nul ne peut relier aucun point à ses voisins.
    results = dbscan_explore.explore_dbscan(df, eps_values=(0.001,), min_samples_values=(5,))

    result = results[0]
    assert result.n_clusters == 0
    assert result.n_noise == len(df)
    assert result.noise_pct == 100.0
    assert result.silhouette is None


def test_explore_dbscan_well_chosen_eps_recovers_the_three_real_groups():
    df = _make_synthetic_dataset(n_per_group=40)

    # Les 3 groupes synthétiques sont très resserrés (scale=0.05) et bien
    # séparés (cf. docstring de _make_synthetic_dataset) : un eps modéré doit
    # les retrouver proprement, avec peu ou pas de bruit.
    results = dbscan_explore.explore_dbscan(df, eps_values=(1.0,), min_samples_values=(5,))

    result = results[0]
    assert result.n_clusters == 3
    assert result.noise_pct < 5.0
    assert result.silhouette is not None
    assert result.silhouette > 0.5


def test_log_candidates_to_mlflow_tracks_all_combinations(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # voir commentaire équivalent dans test_kmeans_model.py
    df = _make_synthetic_dataset(n_per_group=30)
    tracking_uri = f"sqlite:///{tmp_path}/mlflow.db"

    results = dbscan_explore.explore_dbscan(df, eps_values=(0.5, 1.5), min_samples_values=(5,))
    dbscan_explore.log_candidates_to_mlflow(
        results, len(df), experiment_name="test_ve_clustering_dbscan", tracking_uri=tracking_uri
    )

    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.get_experiment_by_name("test_ve_clustering_dbscan")
    assert experiment is not None
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    assert len(runs) == len(results)
    assert set(runs["params.algorithm"]) == {"dbscan"}
