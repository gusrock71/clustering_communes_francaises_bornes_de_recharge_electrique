"""Application Streamlit -- restitution du clustering K-Means (besoin en
bornes de recharge VE par commune), 2026-08-25.

Rôle STRICTEMENT limité à la présentation : cette app n'accède JAMAIS
directement à Postgres/Neon, elle appelle uniquement l'API HTTP
(`ve_pipeline/api/main.py`, déployée sur Cloud Run) -- même séparation
présentation/logique métier que celle actée lors du choix de la stack de
déploiement (2026-08-24). Deux fonctionnalités :
  1. une recherche par nom de commune, code postal ou code INSEE
     (`GET /communes/search`) ;
  2. une carte de l'ensemble des communes, un point par commune coloré selon
     son cluster (`GET /communes/map`), pour visualiser les grands ensembles
     géographiques (ex. "déserts IRVE") plutôt qu'un choroplèthe à base de
     contours communaux -- choix fait pour rester simple et performant sur
     32 101 communes (cf. discussion du 2026-08-24, README.md).

Configuration : l'URL de l'API est lue depuis la variable d'environnement
`API_BASE_URL` (via un fichier `.env` local ou les settings Streamlit Cloud),
avec un repli sur le service Cloud Run déployé le 2026-08-24.
"""

from __future__ import annotations

import os

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DEFAULT_API_BASE_URL = "https://ve-clustering-api-164992414488.europe-west9.run.app"
API_BASE_URL = os.environ.get("API_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/")

# 4 clusters (k=4, modèle retenu -- cf. README.md/Notion) : une couleur fixe
# par cluster_id pour que la légende reste stable d'un rafraîchissement à
# l'autre (ne pas générer les couleurs dynamiquement).
CLUSTER_COLORS: dict[int, list[int]] = {
    0: [31, 119, 180],   # bleu
    1: [255, 127, 14],   # orange
    2: [44, 160, 44],    # vert
    3: [214, 39, 40],    # rouge
}
DEFAULT_COLOR = [128, 128, 128]  # gris -- cluster_id imprévu (garde-fou)
# Gris plus clair que DEFAULT_COLOR pour rester visuellement distinct du
# garde-fou ci-dessus : ce cas est attendu (communes non clusterisées faute
# de données), pas une anomalie.
INSUFFICIENT_DATA_COLOR = [190, 190, 190]

FEATURE_LABELS: dict[str, str] = {
    "part_thermosensible": "Part thermosensible (%)",
    "taux_chauffage_electrique": "Taux de chauffage électrique (%)",
    "croissance_immat_ve_pct": "Croissance immatriculations VE",
    "demarrage_ve_tardif": "Démarrage VE tardif",
    "taux_couverture_afir": "Taux de couverture AFIR",
    "ratio_pdc_par_100_ve": "Ratio PDC / 100 VE (normalisé)",
}

@st.cache_data(ttl=3600, show_spinner=False)
def load_map_data() -> pd.DataFrame:
    """Charge le jeu complet pour la carte. Mis en cache 1h (le clustering
    n'est réentraîné qu'occasionnellement, pas de raison d'appeler l'API à
    chaque interaction utilisateur).

    `cluster_id` peut être `null` (communes avec centroïde connu mais
    exclues du clustering faute de données immatriculations/Enedis
    suffisantes, cf. docstring de `/communes/map` -- ~2 870 communes sur
    34 969, décision du 2026-08-25). Ces communes sont affichées en gris
    ("données insuffisantes") plutôt qu'omises, pour distinguer une vraie
    absence de commune d'une commune non clusterisée. JSON `null` devient
    `None` via `response.json()`, mais pandas le convertit en `NaN` une fois
    dans une colonne -- `pd.isna(c)` couvre les deux cas, `.get(c, ...)` seul
    ne suffirait pas car `NaN` n'est jamais égal à une clé de dict."""
    response = requests.get(f"{API_BASE_URL}/communes/map", timeout=60)
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    df["color"] = df["cluster_id"].map(
        lambda c: INSUFFICIENT_DATA_COLOR if pd.isna(c) else CLUSTER_COLORS.get(int(c), DEFAULT_COLOR)
    )
    # Libellé texte pour le tooltip pydeck : "Cluster {None}" serait affiché
    # tel quel côté JS, d'où ce libellé explicite pour le cas gris.
    df["cluster_label"] = df["cluster_id"].map(
        lambda c: "Données insuffisantes" if pd.isna(c) else f"Cluster {int(c)}"
    )
    return df


def search_communes(query: str, limit: int = 15) -> tuple[list[dict], str | None]:
    """Retourne (résultats, message_erreur). `message_erreur` est None en
    cas de succès (y compris "aucun résultat", traité côté appelant)."""
    try:
        response = requests.get(
            f"{API_BASE_URL}/communes/search", params={"q": query, "limit": limit}, timeout=15
        )
    except requests.RequestException as exc:
        return [], f"Impossible de contacter l'API ({exc})."

    if response.status_code == 404:
        return [], None
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        return [], f"Erreur API ({response.status_code}) : {exc}."
    return response.json(), None


def render_commune_card(commune: dict) -> None:
    nom = commune.get("nom_commune") or "(nom inconnu)"
    code_postal = commune.get("code_postal") or "?"
    cluster_id = commune["cluster_id"]

    with st.container(border=True):
        col_title, col_cluster = st.columns([3, 1])
        with col_title:
            st.subheader(f"{nom} ({code_postal})")
            st.caption(f"Code INSEE : {commune['code_commune']}")
        with col_cluster:
            color = CLUSTER_COLORS.get(cluster_id, DEFAULT_COLOR)
            st.markdown(
                f"<div style='background-color: rgb({color[0]},{color[1]},{color[2]}); "
                f"color: white; padding: 8px; border-radius: 6px; text-align: center;'>"
                f"Cluster {cluster_id}</div>",
                unsafe_allow_html=True,
            )

        cols = st.columns(3)
        feature_items = list(FEATURE_LABELS.items())
        for i, (key, label) in enumerate(feature_items):
            value = commune.get(key)
            display_value = "Oui" if value is True else "Non" if value is False else f"{value:.3g}" if isinstance(value, (int, float)) else value
            cols[i % 3].metric(label, display_value)


def render_app() -> None:
    """Construit l'interface complète. Isolé dans une fonction (plutôt que
    du code au niveau module) pour que `streamlit_app/app.py` reste
    importable dans les tests sans exécuter l'UI -- seul le bloc
    `if __name__ == "__main__"` en bas de fichier l'appelle, or Streamlit
    exécute justement le script cible avec `__name__ == "__main__"` (voir
    `streamlit run app.py`), donc ce garde-fou ne change rien à l'usage
    normal de l'app."""
    st.set_page_config(page_title="Bornes de recharge VE -- clusters communes", layout="wide")

    st.title("Besoins en bornes de recharge VE par commune")
    st.caption(
        "Restitution du clustering K-Means (k=4) -- recherche par commune et vue "
        "cartographique des grands ensembles géographiques."
    )

    st.subheader("Rechercher une commune")
    query = st.text_input(
        "Nom, code postal ou code INSEE",
        placeholder="ex. Lyon, 69001, ou 69123",
        label_visibility="collapsed",
    )

    if query.strip():
        results, error = search_communes(query.strip())
        if error:
            st.error(error)
        elif not results:
            st.info(f"Aucune commune trouvée pour « {query} ».")
        else:
            st.caption(f"{len(results)} résultat(s)")
            for commune in results:
                render_commune_card(commune)

    st.divider()

    st.subheader("Carte des clusters")
    st.caption(
        "Chaque point représente une commune, coloré selon son cluster. Les grands "
        "ensembles de même couleur signalent des zones homogènes (ex. déserts IRVE)."
    )

    legend_cols = st.columns(len(CLUSTER_COLORS) + 1)
    for cluster_id, color in CLUSTER_COLORS.items():
        with legend_cols[cluster_id]:
            st.markdown(
                f"<div style='background-color: rgb({color[0]},{color[1]},{color[2]}); "
                f"color: white; padding: 4px; border-radius: 4px; text-align: center;'>"
                f"Cluster {cluster_id}</div>",
                unsafe_allow_html=True,
            )
    with legend_cols[len(CLUSTER_COLORS)]:
        c = INSUFFICIENT_DATA_COLOR
        st.markdown(
            f"<div style='background-color: rgb({c[0]},{c[1]},{c[2]}); "
            f"color: white; padding: 4px; border-radius: 4px; text-align: center;'>"
            f"Données insuffisantes</div>",
            unsafe_allow_html=True,
        )

    try:
        map_df = load_map_data()
    except requests.RequestException as exc:
        st.error(f"Impossible de charger les données de la carte depuis l'API ({exc}).")
    else:
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["longitude", "latitude"],
            get_fill_color="color",
            get_radius=1500,
            radius_min_pixels=1,
            radius_max_pixels=6,
            pickable=True,
        )
        view_state = pdk.ViewState(latitude=46.6, longitude=2.5, zoom=4.8)
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={"text": "{nom_commune}\n{cluster_label}"},
                map_style=None,
            )
        )
        st.caption(f"{len(map_df)} communes affichées.")


if __name__ == "__main__":
    render_app()
