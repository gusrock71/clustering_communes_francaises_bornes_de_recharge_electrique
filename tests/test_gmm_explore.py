"""Tests de bon fonctionnement de l'exploration GMM (exploratoire, pas le
pipeline de production -- voir tests/test_kmeans_model.py pour K-Means).

Réutilise le même générateur de dataset synthétique à 3 groupes séparés que
test_kmeans_model.py.
"""

from __future__ import annotations

from ve_pipeline.clustering import gmm_explore
from .test_kmeans_model import _make_synthetic_dataset


def test_explore_gmm_returns_one_result_per_combination():
    df = _make_synthetic_dataset(n_per_group=30)

    results = gmm_explore.explore_gmm(df, n_components_values=(3, 4), covariance_types=("full", "diag"))

    assert len(results) == 4  # 2 n_components x 2 covariance_type
    combos = {(r.n_components, r.covariance_type) for r in results}
    assert combos == {(3, "full"), (3, "diag"), (4, "full"), (4, "diag")}


def test_explore_gmm_recovers_the_three_real_groups_with_high_confidence():
    df = _make_synthetic_dataset(n_per_group=40)

    results = gmm_explore.explore_gmm(df, n_components_values=(3,), covariance_types=("full",))

    result = results[0]
    # Les 3 groupes synthétiques sont resserrés et bien séparés : GMM doit
    # les retrouver avec une confiance d'assignation proche de 1 (peu
    # d'ambiguïté entre groupes) et une bonne silhouette.
    assert result.silhouette > 0.5
    assert result.mean_max_proba > 0.95


def test_select_best_candidate_prefers_lower_bic():
    df = _make_synthetic_dataset(n_per_group=40)

    # 3 composantes doit mieux coller aux 3 vrais groupes que 4 (qui doit
    # sur-découper un groupe homogène) -- BIC pénalise cette complexité en trop.
    results = gmm_explore.explore_gmm(df, n_components_values=(3, 4), covariance_types=("full",))

    best = gmm_explore.select_best_candidate(results)
    assert best.n_components == 3
    assert best.bic == min(r.bic for r in results)


def test_log_candidates_to_mlflow_tracks_all_combinations_and_marks_the_best(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # voir commentaire équivalent dans test_kmeans_model.py
    df = _make_synthetic_dataset(n_per_group=30)
    tracking_uri = f"sqlite:///{tmp_path}/mlflow.db"

    results = gmm_explore.explore_gmm(df, n_components_values=(3, 4), covariance_types=("full",))
    gmm_explore.log_candidates_to_mlflow(
        results, len(df), experiment_name="test_ve_clustering_gmm", tracking_uri=tracking_uri
    )

    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.get_experiment_by_name("test_ve_clustering_gmm")
    assert experiment is not None
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    assert len(runs) == len(results)
    assert set(runs["params.algorithm"]) == {"gmm"}
    assert (runs["params.selected"] == "True").sum() == 1
