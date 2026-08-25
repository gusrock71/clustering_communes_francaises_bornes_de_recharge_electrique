"""API FastAPI de lecture des résultats de clustering K-Means (2026-08-24).

Rôle STRICTEMENT limité à la lecture des clusters déjà calculés et écrits en
base par `ve_pipeline.clustering.kmeans_model.write_cluster_assignments`
(table `ml__cluster_assignments`, une ligne = une commune = un cluster_id
figé au dernier entraînement). Cette API ne charge PAS le pipeline sklearn
et n'a AUCUNE dépendance à MLflow en production : le modèle reste un outil
d'entraînement/traçabilité (cf. discussion du 2026-08-24), la mise en
service se fait sur le résultat déjà écrit en base, pas sur une inférence en
direct. Ce choix garde l'image Docker légère (ni boto3, ni duckdb, ni
pyspark, ni mlflow -- voir requirements-api.txt) et évite de dépendre d'un
serveur MLflow toujours actif en production.

Trois tables lues, toutes sur la même base Postgres/Neon que le reste du
pipeline :
  - `ml__cluster_assignments` (résultat du clustering) ;
  - `staging.ref_codes_postaux` (seed dbt La Poste : nom de commune, code
    postal), jointe sur `code_commune` pour permettre une recherche par nom
    ou code postal, pas seulement par code INSEE ;
  - `staging.ref_centroides_communes` (seed dbt, source geo.api.gouv.fr,
    ajouté le 2026-08-25) : latitude/longitude par commune, utilisées par
    `/communes/map` pour la future carte Streamlit.

Les noms de ces tables sont surchargeables par variable d'environnement
(`CLUSTER_TABLE`, `COMMUNES_REF_TABLE`, `CENTROIDS_TABLE`) -- utile pour les
tests, qui tournent sur SQLite (pas de schéma séparé, donc les tables sont
référencées sans préfixe `staging.`, cf. tests/test_api.py) plutôt que sur
Postgres.
"""

from __future__ import annotations

import os
import re
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

DEFAULT_CLUSTER_TABLE = "ml__cluster_assignments"
DEFAULT_COMMUNES_REF_TABLE = "staging.ref_codes_postaux"
DEFAULT_CENTROIDS_TABLE = "staging.ref_centroides_communes"


def _cluster_table() -> str:
    return os.environ.get("CLUSTER_TABLE", DEFAULT_CLUSTER_TABLE)


def _communes_ref_table() -> str:
    return os.environ.get("COMMUNES_REF_TABLE", DEFAULT_COMMUNES_REF_TABLE)


def _centroids_table() -> str:
    return os.environ.get("CENTROIDS_TABLE", DEFAULT_CENTROIDS_TABLE)


# Code INSEE commune : 5 chiffres, ou 2A/2B pour la Corse (ex: "2A004").
_CODE_PATTERN = re.compile(r"^(\d{5}|2[AB]\d{3})$")

app = FastAPI(
    title="VE Pipeline -- API clustering communes",
    description="Sert les clusters de communes (besoin en bornes de recharge VE) déjà calculés par le pipeline K-Means.",
    version="1.0.0",
)

_engine: Engine | None = None


def get_db_engine() -> Engine:
    """Engine SQLAlchemy paresseux (créé au premier appel, réutilisé ensuite).

    Volontairement indépendant de `ve_pipeline.loading.postgres_loader.get_engine`
    (qui importe `ve_pipeline.ingestion`, donc boto3) : cette API n'a besoin
    que de lire Postgres, pas d'accéder à S3, et garder les deux chemins
    séparés évite d'alourdir l'image Docker de service avec des dépendances
    inutiles à l'exécution.
    """
    global _engine
    if _engine is None:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError(
                "DATABASE_URL non défini -- ex. postgresql+psycopg2://user:password@host/dbname (Neon)."
            )
        _engine = create_engine(dsn)
    return _engine


def reset_db_engine() -> None:
    """Force la recréation de l'engine au prochain appel -- utilisé par les
    tests pour changer de DATABASE_URL entre deux cas de test."""
    global _engine
    _engine = None


class CommuneMapPoint(BaseModel):
    code_commune: str
    nom_commune: str | None = None
    cluster_id: int | None = None
    latitude: float
    longitude: float


class CommuneCluster(BaseModel):
    code_commune: str
    nom_commune: str | None = None
    code_postal: str | None = None
    cluster_id: int
    part_thermosensible: float
    taux_chauffage_electrique: float
    croissance_immat_ve_pct: float
    demarrage_ve_tardif: bool
    taux_couverture_afir: float
    ratio_pdc_par_100_ve: float


_SELECT_COLUMNS = """
    c.code_commune,
    p.nom_de_la_commune AS nom_commune,
    p.code_postal,
    c.cluster_id,
    c.part_thermosensible,
    c.taux_chauffage_electrique,
    c.croissance_immat_ve_pct,
    c.demarrage_ve_tardif,
    c.taux_couverture_afir,
    c.ratio_pdc_par_100_ve
"""

# Certaines communes ont plusieurs lignes dans ref_codes_postaux (plusieurs
# codes postaux, ex. grandes villes à arrondissements) : une recherche par
# nom peut donc renvoyer la même commune plusieurs fois, une fois par code
# postal -- accepté tel quel, c'est une information utile (pas un doublon
# arbitraire) pour une recherche postale. La recherche par code exact
# (`/communes/{code_commune}`) n'est pas concernée : `LIMIT 1` y règle le cas.
def _join_clause() -> str:
    return f"""
    FROM {_cluster_table()} c
    LEFT JOIN {_communes_ref_table()} p ON p.code_commune_insee = c.code_commune
"""


def _row_to_model(row: Any) -> CommuneCluster:
    return CommuneCluster(
        code_commune=row.code_commune,
        nom_commune=row.nom_commune,
        code_postal=row.code_postal,
        cluster_id=int(row.cluster_id),
        part_thermosensible=float(row.part_thermosensible),
        taux_chauffage_electrique=float(row.taux_chauffage_electrique),
        croissance_immat_ve_pct=float(row.croissance_immat_ve_pct),
        demarrage_ve_tardif=bool(row.demarrage_ve_tardif),
        taux_couverture_afir=float(row.taux_couverture_afir),
        ratio_pdc_par_100_ve=float(row.ratio_pdc_par_100_ve),
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/communes/search", response_model=list[CommuneCluster])
def search_communes(
    q: str = Query(..., min_length=2, description="Nom de commune, code postal ou code INSEE"),
    limit: int = Query(20, ge=1, le=100),
) -> list[CommuneCluster]:
    """Recherche par code (INSEE ou postal, match exact) si `q` ressemble à
    un code, sinon par nom (sous-chaîne, insensible à la casse/aux accents
    déjà absents dans le référentiel La Poste qui est tout en majuscules)."""
    engine = get_db_engine()
    q_stripped = q.strip()

    with engine.connect() as conn:
        if _CODE_PATTERN.match(q_stripped):
            # GROUP BY + MIN() : contrairement à la recherche par nom
            # (duplication volontaire, voir plus bas), un match par code doit
            # renvoyer UNE ligne par commune -- une commune à plusieurs codes
            # postaux ne doit pas apparaître 2 fois pour une recherche par
            # code INSEE. MIN(nom/code_postal) est déterministe et portable
            # SQLite/Postgres (pas de DISTINCT ON, spécifique Postgres).
            query = text(f"""
                SELECT
                    c.code_commune,
                    MIN(p.nom_de_la_commune) AS nom_commune,
                    MIN(p.code_postal) AS code_postal,
                    c.cluster_id,
                    c.part_thermosensible,
                    c.taux_chauffage_electrique,
                    c.croissance_immat_ve_pct,
                    c.demarrage_ve_tardif,
                    c.taux_couverture_afir,
                    c.ratio_pdc_par_100_ve
                {_join_clause()}
                WHERE c.code_commune = :q OR p.code_postal = :q
                GROUP BY c.code_commune, c.cluster_id, c.part_thermosensible, c.taux_chauffage_electrique,
                         c.croissance_immat_ve_pct, c.demarrage_ve_tardif, c.taux_couverture_afir,
                         c.ratio_pdc_par_100_ve
                LIMIT :limit
            """)
            rows = conn.execute(query, {"q": q_stripped, "limit": limit}).fetchall()
        else:
            # UPPER() ... LIKE plutôt que ILIKE (spécifique Postgres) : fonctionne
            # à l'identique sur SQLite (tests) et Postgres (prod).
            query = text(f"""
                SELECT {_SELECT_COLUMNS}
                {_join_clause()}
                WHERE UPPER(p.nom_de_la_commune) LIKE :pattern
                LIMIT :limit
            """)
            rows = conn.execute(query, {"pattern": f"%{q_stripped.upper()}%", "limit": limit}).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Aucune commune trouvée pour '{q}'")
    return [_row_to_model(r) for r in rows]


@app.get("/communes/map", response_model=list[CommuneMapPoint])
def get_communes_map() -> list[CommuneMapPoint]:
    """Jeu complet (une ligne par commune AVEC centroïde connu) pour la
    carte Streamlit : code, nom, cluster et coordonnées.

    Part de `ref_centroides_communes` (univers quasi complet des communes
    françaises, 34 969) plutôt que de `ml__cluster_assignments` (32 101 --
    voir `mart_clustering_dataset.sql`, filtre `has_immatriculations = true
    AND has_enedis = true`) : les ~2 870 communes qui manquent de données
    Enedis ou immatriculations suffisantes n'ont jamais été clusterisées,
    mais doivent quand même apparaître sur la carte (`cluster_id = null`)
    plutôt que de créer des trous silencieux -- constat fait le 2026-08-25
    en visualisant la première version de cette carte (zones blanches
    inexpliquées sans cette distinction)."""
    engine = get_db_engine()
    query = text(f"""
        SELECT
            ctr.code_commune,
            MIN(p.nom_de_la_commune) AS nom_commune,
            c.cluster_id,
            ctr.latitude,
            ctr.longitude
        FROM {_centroids_table()} ctr
        LEFT JOIN {_cluster_table()} c ON c.code_commune = ctr.code_commune
        LEFT JOIN {_communes_ref_table()} p ON p.code_commune_insee = ctr.code_commune
        GROUP BY ctr.code_commune, c.cluster_id, ctr.latitude, ctr.longitude
    """)
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return [
        CommuneMapPoint(
            code_commune=r.code_commune,
            nom_commune=r.nom_commune,
            cluster_id=int(r.cluster_id) if r.cluster_id is not None else None,
            latitude=float(r.latitude),
            longitude=float(r.longitude),
        )
        for r in rows
    ]


@app.get("/communes/{code_commune}", response_model=CommuneCluster)
def get_commune(code_commune: str) -> CommuneCluster:
    engine = get_db_engine()
    query = text(f"""
        SELECT {_SELECT_COLUMNS}
        {_join_clause()}
        WHERE c.code_commune = :code_commune
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"code_commune": code_commune}).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Commune '{code_commune}' introuvable dans {_cluster_table()}")
    return _row_to_model(row)
