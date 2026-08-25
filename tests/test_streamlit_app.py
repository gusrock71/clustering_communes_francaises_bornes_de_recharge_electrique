"""Tests de la logique testable de l'application Streamlit
(streamlit_app/app.py). L'UI elle-même (render_app) n'est pas testée ici --
elle est isolée dans une fonction non appelée à l'import (cf. docstring de
render_app) précisément pour permettre de tester `search_communes` et le
mapping des couleurs sans dépendre d'un contexte Streamlit actif.
"""

from __future__ import annotations

import requests
import responses

from streamlit_app import app as streamlit_app


def test_search_communes_returns_results_on_success():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{streamlit_app.API_BASE_URL}/communes/search",
            json=[{"code_commune": "01001", "nom_commune": "L ABERGEMENT CLEMENCIAT", "cluster_id": 2}],
            status=200,
        )
        results, error = streamlit_app.search_communes("abergement")

    assert error is None
    assert len(results) == 1
    assert results[0]["code_commune"] == "01001"


def test_search_communes_returns_empty_without_error_on_404():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{streamlit_app.API_BASE_URL}/communes/search",
            json={"detail": "not found"},
            status=404,
        )
        results, error = streamlit_app.search_communes("introuvable")

    assert results == []
    assert error is None  # 404 = "aucun résultat", pas une erreur à afficher en rouge


def test_search_communes_returns_error_message_on_server_error():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{streamlit_app.API_BASE_URL}/communes/search",
            json={"detail": "boom"},
            status=500,
        )
        results, error = streamlit_app.search_communes("paris")

    assert results == []
    assert error is not None
    assert "500" in error


def test_search_communes_returns_error_message_on_connection_failure():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{streamlit_app.API_BASE_URL}/communes/search",
            body=requests.exceptions.ConnectionError("refused"),
        )
        results, error = streamlit_app.search_communes("paris")

    assert results == []
    assert error is not None
    assert "Impossible de contacter l'API" in error


def test_cluster_colors_cover_the_four_retained_clusters():
    # k=4 est le modèle retenu (cf. README.md) -- la légende doit couvrir
    # exactement les cluster_id 0 à 3, pas plus pas moins.
    assert set(streamlit_app.CLUSTER_COLORS.keys()) == {0, 1, 2, 3}


def test_load_map_data_colors_null_cluster_as_insufficient_data():
    # cluster_id = null (communes exclues du clustering, cf. main.py) doit
    # être coloré en gris "données insuffisantes", pas planter ni retomber
    # sur DEFAULT_COLOR (réservé aux cluster_id inattendus, cf. commentaire
    # dans app.py).
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{streamlit_app.API_BASE_URL}/communes/map",
            json=[
                {"code_commune": "01001", "nom_commune": "A", "cluster_id": 2, "latitude": 1.0, "longitude": 1.0},
                {"code_commune": "12345", "nom_commune": "B", "cluster_id": None, "latitude": 2.0, "longitude": 2.0},
            ],
            status=200,
        )
        streamlit_app.load_map_data.clear()
        df = streamlit_app.load_map_data()

    by_code = df.set_index("code_commune")
    assert by_code.loc["01001", "color"] == streamlit_app.CLUSTER_COLORS[2]
    assert by_code.loc["12345", "color"] == streamlit_app.INSUFFICIENT_DATA_COLOR
    assert by_code.loc["12345", "cluster_label"] == "Données insuffisantes"
