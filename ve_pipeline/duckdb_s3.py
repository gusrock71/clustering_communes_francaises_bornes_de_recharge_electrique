"""Configuration DuckDB <-> S3, partagée par les étapes du pipeline qui lisent
ou écrivent sur S3 via DuckDB (cleaning, jointure, et plus tard features/J3).
"""

from __future__ import annotations

import duckdb


def configure_s3(con: duckdb.DuckDBPyConnection, region: str = "eu-west-3") -> None:
    """Charge httpfs + les credentials AWS depuis l'environnement/~/.aws (aucun
    secret en dur ici)."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    con.execute(f"SET s3_region='{region}';")
