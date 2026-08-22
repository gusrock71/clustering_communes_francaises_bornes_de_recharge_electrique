"""Exploration PySpark de la landing zone S3 du pipeline VE.

Ouvre en lecture les fichiers bruts (`raw/<source>/dt=.../`) et, s'il existe
déjà, le parquet de jointure territoriale (`staging/territorial/dt=.../`),
et affiche schéma + 10 premières lignes (converties en pandas pour un rendu
lisible) + % de valeurs nulles par colonne. Rien n'est écrit : script de
découverte, pas une étape du pipeline.

Prérequis :
    pip install -r requirements.txt   # pyspark, python-dotenv, pandas
    Credentials AWS dans .env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION)
    ou dans l'environnement — mêmes credentials que ceux utilisés par boto3
    pour le run d'ingestion réel.

Usage :
    python3 scripts/explore_s3_pyspark.py
    python3 scripts/explore_s3_pyspark.py --bucket ve-pipeline-landing --dt 2026-08-16

Si --dt est omis, le script prend automatiquement la partition dt= la plus
récente disponible pour chaque source (elles peuvent différer d'une source
à l'autre si les runs n'ont pas eu lieu le même jour).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ve_pipeline.ingestion.config import SOURCES
from ve_pipeline.ingestion.s3_landing import count_objects_with_prefix, get_s3_client

load_dotenv()  # charge .env à la racine du projet (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION)

DEFAULT_BUCKET = "ve-pipeline-landing"
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")

# Rendu pandas : on veut TOUTES les colonnes (display.max_columns=None,
# sinon pandas cache celles du milieu avec "..."), mais sans repartir sur un
# print(df.to_string()) illisible sur une ligne géante. On laisse
# display.width non fixé : pandas détecte la largeur du terminal et, avec
# expand_frame_repr=True (défaut), découpe automatiquement l'affichage en
# plusieurs blocs de colonnes empilés verticalement quand ça ne tient pas —
# toutes les colonnes restent visibles, juste sur plusieurs blocs. On limite
# seulement la largeur d'une cellule (ex: "observations" IRVE, longue et peu
# lisible) pour ne pas élargir inutilement chaque bloc.
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 40)

# Séparateur CSV réel par fichier, vérifié sur un run réel le 2026-08-17 (voir
# mémoire projet) : IRVE est en ',' (CSV standard, schéma etalab), les 2
# fichiers immatriculations sont en ';', Enedis en ','.
SEP_BY_FILE_KEY = {
    "irve_consolide": ",",
    "immatriculations_neuf": ";",
    "immatriculations_occasion": ";",
    "enedis_conso_commune_2024": ",",
}


def raw_files():
    """Chaque (nom_source_s3, clé_fichier, extension, séparateur) réellement
    déposé par le connecteur d'ingestion (ve_pipeline/ingestion/connector.py) :
    `raw/<source.name>/dt=.../<file_cfg.key>.<ext>`.

    Important : le nom de dossier S3 est `source.name` (ex: "irve",
    "immatriculations"), PAS `file_cfg.key` (ex: "irve_consolide",
    "immatriculations_neuf") — les deux fichiers immatriculations
    (neuf/occasion) partagent le même dossier source.
    """
    for source in SOURCES.values():
        for file_cfg in source.files:
            sep = SEP_BY_FILE_KEY.get(file_cfg.key, ";")
            yield source.name, file_cfg.key, file_cfg.expected_ext, sep


def latest_partition(s3_client, bucket: str, prefix: str) -> str | None:
    """Retourne le nom de la partition dt= la plus récente sous un préfixe."""
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    dts = [
        cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        for cp in resp.get("CommonPrefixes", [])
        if cp["Prefix"].rstrip("/").rsplit("/", 1)[-1].startswith("dt=")
    ]
    return sorted(dts)[-1] if dts else None


# Version de hadoop-aws à FAIRE CORRESPONDRE à la version de Hadoop embarquée
# dans votre PySpark (voir les jars hadoop-client-api-*.jar /
# hadoop-client-runtime-*.jar dans le dossier `jars/` de l'installation
# pyspark). Un hadoop-aws plus ancien que le Hadoop embarqué plante avec des
# erreurs de parsing de config (ex: NumberFormatException sur "60s") car
# depuis Hadoop 3.4/3.5, S3A est passé à l'AWS SDK v2 : ne pas ajouter
# manuellement com.amazonaws:aws-java-sdk-bundle (SDK v1, obsolète), Ivy
# résout automatiquement le bon software.amazon.awssdk:bundle en transitif.
HADOOP_AWS_VERSION = "3.5.0"


def build_spark(bucket: str) -> SparkSession:
    return (
        SparkSession.builder.appName("ve_pipeline_explore")
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}")
        # Pas besoin de forcer fs.s3a.aws.credentials.provider : la chaîne par
        # défaut de S3A essaie déjà les variables d'environnement
        # (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, chargées depuis .env par
        # load_dotenv() plus haut), puis un profil AWS, puis IAM instance role.
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        # Hadoop 3.5.0 active par défaut le nouvel "Analytics Accelerator"
        # (encore Alpha, HADOOP-19559) comme implémentation de lecture S3A —
        # il plante (IndexOutOfBoundsException dans StreamReader) sur des
        # lectures CSV classiques. On repasse sur l'implémentation "classic",
        # stable, largement suffisante pour de l'exploration.
        .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
        .getOrCreate()
    )


def null_percentage_report(df: DataFrame, total_rows: int) -> pd.DataFrame:
    """Petit dataframe pandas (colonne, pct_null) trié du plus au moins vide."""
    if total_rows == 0:
        return pd.DataFrame(columns=["colonne", "pct_null"])

    null_counts = df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    ).first()

    rows = [(c, round(100.0 * null_counts[c] / total_rows, 1)) for c in df.columns]
    rows.sort(key=lambda r: r[1], reverse=True)
    return pd.DataFrame(rows, columns=["colonne", "pct_null"])


def describe(df: DataFrame, label: str) -> None:
    """Affiche schéma, 10 premières lignes (pandas) et % de nulls par colonne."""
    df.printSchema()

    print(f"10 premières lignes ({label}) :")
    # print(df) (pas .to_string()) : c'est ce qui déclenche le rendu pandas
    # "standard" (comme dans un notebook), avec troncature automatique des
    # colonnes du milieu ("...") si la table est trop large pour le terminal.
    # .to_string() force au contraire TOUT afficher sur une ligne géante.
    print(df.limit(10).toPandas())
    print()

    total_rows = df.count()
    print(f"{total_rows} lignes")

    print(f"% de valeurs nulles par colonne ({label}) :")
    # Ici la table ne fait que 2 colonnes : to_string(index=False) reste lisible
    # et évite d'afficher l'index pandas, inutile pour ce petit récapitulatif.
    print(null_percentage_report(df, total_rows).to_string(index=False))
    print()


def explore(bucket: str, dt: str | None) -> None:
    # Le nom de partition S3 réel est "dt=YYYY-MM-DD" (voir landing_key() dans
    # s3_landing.py) ; si l'utilisateur passe --dt 2026-08-16 sans le préfixe,
    # on le rajoute pour ne pas construire un chemin S3 inexistant.
    if dt and not dt.startswith("dt="):
        dt = f"dt={dt}"

    s3_client = get_s3_client()
    spark = build_spark(bucket)

    print(f"Bucket : s3a://{bucket}\n")

    for source_name, file_key, ext, sep in raw_files():
        prefix = f"raw/{source_name}/"
        source_dt = dt or latest_partition(s3_client, bucket, prefix)
        if source_dt is None:
            print(f"[{source_name}/{file_key}] aucune partition trouvée sous {prefix}, ignoré.\n")
            continue

        path = f"s3a://{bucket}/{prefix}{source_dt}/{file_key}.{ext}"
        label = f"{source_name}/{file_key}"
        print(f"=== {label} ({path}) ===")
        df = (
            spark.read.option("header", True)
            .option("sep", sep)
            .option("inferSchema", False)  # exploration = lecture brute en string, pas de guess de type
            .csv(path)
        )
        describe(df, label)

    staging_prefix = "staging/territorial/"
    staging_dt = dt or latest_partition(s3_client, bucket, staging_prefix)
    staging_key_prefix = f"{staging_prefix}{staging_dt}/" if staging_dt else None

    # Vérifié via boto3 (pas besoin d'aller jusqu'à Spark) : évite un
    # PATH_NOT_FOUND bruyant si J2 n'a pas encore tourné sur cette dt/ce bucket.
    if staging_key_prefix and count_objects_with_prefix(s3_client, bucket, staging_key_prefix) > 0:
        path = f"s3a://{bucket}/{staging_key_prefix}territorial.parquet"
        print(f"=== staging_territorial ({path}) ===")
        df = spark.read.parquet(path)
        describe(df, "staging_territorial")
    else:
        print("Pas de staging_territorial trouvé pour cette dt (J2 pas encore exécuté sur ce bucket/cette date).")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--dt", default=None, help="Partition dt=YYYY-MM-DD à forcer (sinon : la plus récente par source)")
    args = parser.parse_args()
    explore(args.bucket, args.dt)


if __name__ == "__main__":
    main()
