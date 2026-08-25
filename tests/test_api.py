"""Tests de l'API FastAPI de lecture des clusters (ve_pipeline/api/main.py).

Même convention que test_postgres_loader.py : SQLite (fichier, via tmp_path)
comme stand-in de Postgres. SQLite n'a pas de schémas séparés, donc
`COMMUNES_REF_TABLE` est surchargé à "ref_codes_postaux" (sans préfixe
"staging.") pour ces tests uniquement -- le comportement de jointure testé
est identique à la production.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from ve_pipeline.api import main as api_main

CLUSTER_ROWS = [
    {
        "code_commune": "01001",
        "part_thermosensible": 0.42,
        "taux_chauffage_electrique": 0.31,
        "croissance_immat_ve_pct": 1.25,
        "demarrage_ve_tardif": 0,
        "taux_couverture_afir": 0.8,
        "ratio_pdc_par_100_ve": 1.1,
        "cluster_id": 2,
        "mlflow_run_id": "test-run",
        "trained_at": "2026-08-22T00:00:00+00:00",
    },
    {
        "code_commune": "75056",
        "part_thermosensible": 0.10,
        "taux_chauffage_electrique": 0.05,
        "croissance_immat_ve_pct": 0.60,
        "demarrage_ve_tardif": 0,
        "taux_couverture_afir": 1.4,
        "ratio_pdc_par_100_ve": 2.0,
        "cluster_id": 0,
        "mlflow_run_id": "test-run",
        "trained_at": "2026-08-22T00:00:00+00:00",
    },
    {
        # Commune sans centroïde connu (absente de CENTROIDS_ROWS) -- sert à
        # vérifier que /communes/map l'exclut plutôt que de renvoyer des
        # coordonnées nulles.
        "code_commune": "88888",
        "part_thermosensible": 0.20,
        "taux_chauffage_electrique": 0.15,
        "croissance_immat_ve_pct": 0.30,
        "demarrage_ve_tardif": 1,
        "taux_couverture_afir": 0.5,
        "ratio_pdc_par_100_ve": 0.6,
        "cluster_id": 1,
        "mlflow_run_id": "test-run",
        "trained_at": "2026-08-22T00:00:00+00:00",
    },
]

COMMUNES_ROWS = [
    {
        "code_commune_insee": "01001",
        "nom_de_la_commune": "L ABERGEMENT CLEMENCIAT",
        "code_postal": "01400",
        "libelle_d_acheminement": "L ABERGEMENT CLEMENCIAT",
        "ligne_5": None,
    },
    {
        "code_commune_insee": "75056",
        "nom_de_la_commune": "PARIS",
        "code_postal": "75001",
        "libelle_d_acheminement": "PARIS 1",
        "ligne_5": None,
    },
    {
        # Deuxième code postal pour la même commune (Paris) -- vérifie que la
        # recherche par nom ne casse pas sur les doublons de code_commune.
        "code_commune_insee": "75056",
        "nom_de_la_commune": "PARIS",
        "code_postal": "75002",
        "libelle_d_acheminement": "PARIS 2",
        "ligne_5": None,
    },
]

CENTROIDS_ROWS = [
    {"code_commune": "01001", "latitude": 46.1517, "longitude": 4.9306},
    {"code_commune": "75056", "latitude": 48.8566, "longitude": 2.3522},
    # 88888 volontairement absent : sert à vérifier qu'une commune sans
    # centroïde connu est exclue de /communes/map plutôt que renvoyée avec
    # des coordonnées nulles.
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    dsn = f"sqlite:///{tmp_path}/api_test.db"
    engine = create_engine(dsn)
    pd.DataFrame(CLUSTER_ROWS).to_sql("ml__cluster_assignments", engine, index=False)
    pd.DataFrame(COMMUNES_ROWS).to_sql("ref_codes_postaux", engine, index=False)
    pd.DataFrame(CENTROIDS_ROWS).to_sql("ref_centroides_communes", engine, index=False)

    monkeypatch.setenv("DATABASE_URL", dsn)
    monkeypatch.setenv("COMMUNES_REF_TABLE", "ref_codes_postaux")
    monkeypatch.setenv("CENTROIDS_TABLE", "ref_centroides_communes")
    api_main.reset_db_engine()

    with TestClient(api_main.app) as test_client:
        yield test_client

    api_main.reset_db_engine()


def test_health_returns_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_commune_by_code_insee_returns_cluster(client):
    response = client.get("/communes/01001")
    assert response.status_code == 200
    body = response.json()
    assert body["code_commune"] == "01001"
    assert body["nom_commune"] == "L ABERGEMENT CLEMENCIAT"
    assert body["cluster_id"] == 2


def test_get_commune_unknown_code_returns_404(client):
    response = client.get("/communes/99999")
    assert response.status_code == 404


def test_search_by_code_insee_matches_exact_code(client):
    response = client.get("/communes/search", params={"q": "75056"})
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["code_commune"] == "75056"


def test_search_by_code_postal_matches_exact_code(client):
    response = client.get("/communes/search", params={"q": "01400"})
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["code_commune"] == "01001"


def test_search_by_name_is_case_insensitive_and_partial(client):
    response = client.get("/communes/search", params={"q": "aris"})
    assert response.status_code == 200
    results = response.json()
    # PARIS a 2 lignes dans le référentiel (2 codes postaux) -> 2 résultats,
    # comportement documenté dans main.py (pas un bug).
    assert len(results) == 2
    assert {r["code_postal"] for r in results} == {"75001", "75002"}


def test_search_with_no_match_returns_404(client):
    response = client.get("/communes/search", params={"q": "INTROUVABLE"})
    assert response.status_code == 404


def test_map_returns_one_point_per_commune_with_known_centroid(client):
    response = client.get("/communes/map")
    assert response.status_code == 200
    results = response.json()
    # 3 communes dans ml__cluster_assignments (01001, 75056, 88888), mais
    # seules 01001 et 75056 ont un centroïde connu -- 88888 doit être exclue.
    assert {r["code_commune"] for r in results} == {"01001", "75056"}


def test_map_point_has_correct_coordinates_and_cluster(client):
    response = client.get("/communes/map")
    by_code = {r["code_commune"]: r for r in response.json()}

    assert by_code["01001"]["latitude"] == 46.1517
    assert by_code["01001"]["longitude"] == 4.9306
    assert by_code["01001"]["cluster_id"] == 2
    # Paris a 2 codes postaux dans le référentiel : /communes/map ne doit
    # renvoyer qu'une seule ligne pour 75056, pas une par code postal.
    assert by_code["75056"]["nom_commune"] == "PARIS"
