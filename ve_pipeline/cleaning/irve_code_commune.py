"""[SAUVEGARDE / RÉFÉRENCE — remplacé par dbt le 2026-08-21]

Ce module (dédup des points de charge + reconstitution des
code_insee_commune manquants via regex sur adresse_station + référentiel La
Poste) est désormais porté en dbt : `dbt/models/intermediate/int_irve_cleaned.sql`,
avec le référentiel La Poste chargé comme seed dbt
(`dbt/seeds/ref_codes_postaux.csv`, reconverti en UTF-8 depuis l'export
Latin-1 d'origine `019HexaSmal.csv`). Même logique métier (même clé de
dédup avec repli id_pdc_local, même résolution CP unique/ambigu/non résolu),
validée contre les mêmes cas de test (tests/test_cleaning_irve_code_commune.py)
avant la bascule. Gardé en sauvegarde/référence à la demande explicite de
l'utilisateur.

--- Docstring original ---

Nettoyage IRVE (étape "cleaning", en amont de la jointure J2) : reconstitue
les `code_insee_commune` manquants à partir du texte libre de la colonne
`adresse_station`, et écrit un fichier IRVE nettoyé dans une zone S3 dédiée
(`cleaned/irve/dt=.../`), à côté de la zone brute `raw/irve/dt=.../` produite
par l'ingestion (J1).

Constat sur les vraies données (2026-08-16, fichier fourni par l'utilisateur,
224 267 pdc) : 56 729 pdc (25,3%) n'ont pas de `code_insee_commune`. Mais
l'inspection des adresses réelles montre que ce n'est PAS qu'un trou de
données à combler : une bonne part de ces lignes correspondent à des bornes
hors de France (ex: "203 Herbesthaler Straße, 4700 Eupen" en Belgique, "34 C.
San Roque, 45530 Santa Olalla" en Espagne), remontées par des agrégateurs
paneuropéens (gireve-2, eco-movement...) dans le flux consolidé data.gouv.fr.
Un simple géocodage "au plus proche" les rattacherait à tort à une commune
française.

Stratégie retenue : s'appuyer sur le référentiel officiel des codes postaux
français (La Poste, "Base officielle des codes postaux") comme filtre ET
comme source de correspondance en une seule étape :
  1. Extraire, par regex, les suites de 4-5 chiffres présentes dans
     `adresse_station` (candidats code postal), sans présumer de leur
     position dans la chaîne (l'ordre "CP puis commune" ou "commune puis CP"
     varie selon l'opérateur qui a saisi l'adresse).
  2. Chercher ces candidats dans le référentiel FRANÇAIS des codes postaux.
     Un candidat qui n'y figure pas n'est presque certainement pas un code
     postal français (ex: 4700 Eupen, 45530 Santa Olalla) -> la ligne reste
     non résolue, ce qui est le comportement voulu (pas de fausse commune
     française assignée à une borne étrangère).
  3. Quand un code postal correspond à une seule commune française : on la
     retient directement (`methode='cp_unique'`).
  4. Quand un code postal est partagé par plusieurs communes (fréquent en
     zone rurale) : on désambiguïse en cherchant si le nom de la commune
     apparaît (normalisé : majuscules, sans accents) dans le texte de
     l'adresse (`methode='cp_nom_trouve_dans_adresse'`).
  5. Sinon : non résolu (`methode='non_resolu'`), laissé de côté plutôt que
     deviné.

Référentiel requis (à télécharger une fois, hors sandbox — pas d'accès réseau
sortant ici) : "Base officielle des codes postaux" (La Poste / data.gouv.fr)
  https://www.data.gouv.fr/api/1/datasets/r/008a2dda-2c60-4b63-b910-998f6f818089
Format réel confirmé : CSV séparé par `;`, colonnes
  Code_commune_INSEE;Nom_de_la_commune;Code_postal;Libelle_d_acheminement;Ligne_5
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

import duckdb

from ve_pipeline.duckdb_s3 import configure_s3

logger = logging.getLogger(__name__)


class ReferentielSchemaError(RuntimeError):
    """Le référentiel des codes postaux n'a pas les colonnes attendues."""


def _detect_ref_codes_postaux_columns(con: duckdb.DuckDBPyConnection, path: str) -> dict[str, str]:
    """Lit le header réel du référentiel plutôt que de deviner les noms de
    colonnes -- l'export "Base officielle des codes postaux" a été observé
    avec un `#` en préfixe de la première colonne selon la version téléchargée
    (ex: "#Code_commune_INSEE" au lieu de "Code_commune_INSEE"), et un
    encodage Latin-1 (pas UTF-8, sinon les accents de "Libellé_d_acheminement"
    sont corrompus). On sonde juste les noms de colonnes ici, sans lire les
    données.
    """
    columns = [
        row[0]
        for row in con.execute(
            f"SELECT * FROM read_csv('{path}', delim=';', header=true, encoding='latin-1') LIMIT 0"
        ).description
    ]

    def _find(suffix: str) -> str:
        matches = [c for c in columns if c.lstrip("#").strip() == suffix]
        if not matches:
            raise ReferentielSchemaError(
                f"Colonne '{suffix}' introuvable dans le référentiel des codes postaux "
                f"({path}). Colonnes réelles : {columns}. Le format de l'export a "
                "peut-être changé -- vérifier le fichier téléchargé."
            )
        return matches[0]

    return {
        "code_insee": _find("Code_commune_INSEE"),
        "nom_commune": _find("Nom_de_la_commune"),
        "code_postal": _find("Code_postal"),
    }


def build_reconstitution_views(
    con: duckdb.DuckDBPyConnection,
    irve_path: str,
    ref_codes_postaux_path: str,
) -> None:
    con.execute(f"""
        CREATE OR REPLACE VIEW irve_brut AS
        SELECT *
        FROM read_csv_auto(
            '{irve_path}', union_by_name=true,
            types={{'code_insee_commune': 'VARCHAR', 'puissance_nominale': 'VARCHAR'}}
        );
    """)

    ref_cols = _detect_ref_codes_postaux_columns(con, ref_codes_postaux_path)
    con.execute(f"""
        CREATE OR REPLACE VIEW ref_codes_postaux AS
        SELECT
            "{ref_cols['code_insee']}" AS code_insee,
            "{ref_cols['nom_commune']}" AS nom_commune,
            "{ref_cols['code_postal']}" AS code_postal,
            upper(strip_accents("{ref_cols['nom_commune']}")) AS nom_commune_normalise
        FROM read_csv(
            '{ref_codes_postaux_path}', delim=';', header=true, encoding='latin-1',
            types={{'{ref_cols["code_postal"]}': 'VARCHAR', '{ref_cols["code_insee"]}': 'VARCHAR'}}
        );
    """)

    # Une ligne par pdc sans code commune, avec ses candidats CP extraits du
    # texte libre (peu importe leur position dans l'adresse).
    con.execute(r"""
        CREATE OR REPLACE VIEW irve_candidats_cp AS
        SELECT
            id_pdc_itinerance,
            adresse_station,
            upper(strip_accents(adresse_station)) AS adresse_normalisee,
            unnest(regexp_extract_all(adresse_station, '\b\d{4,5}\b')) AS cp_candidat
        FROM irve_brut
        WHERE code_insee_commune IS NULL OR code_insee_commune = '';
    """)

    # Rapproche chaque candidat CP du référentiel français, et calcule pour
    # chaque candidat : la taille du groupe de communes partageant ce CP, et
    # si le nom de la commune apparaît dans l'adresse.
    con.execute("""
        CREATE OR REPLACE VIEW irve_candidats_matches AS
        SELECT
            c.id_pdc_itinerance,
            r.code_insee,
            r.nom_commune,
            r.code_postal,
            COUNT(*) OVER (PARTITION BY c.id_pdc_itinerance, r.code_postal) AS taille_groupe_cp,
            (position(r.nom_commune_normalise IN c.adresse_normalisee) > 0) AS nom_trouve_dans_adresse
        FROM irve_candidats_cp c
        JOIN ref_codes_postaux r ON r.code_postal = c.cp_candidat;
    """)

    # Sélectionne le meilleur candidat par pdc : priorité au nom trouvé dans
    # l'adresse, puis au groupe le plus petit (CP le moins ambigu).
    con.execute("""
        CREATE OR REPLACE VIEW irve_code_commune_reconstitue AS
        SELECT
            id_pdc_itinerance,
            code_insee AS code_commune_reconstitue,
            CASE
                WHEN taille_groupe_cp = 1 THEN 'cp_unique'
                WHEN nom_trouve_dans_adresse THEN 'cp_nom_trouve_dans_adresse'
                ELSE 'cp_ambigu_non_resolu'
            END AS methode
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY id_pdc_itinerance
                       ORDER BY nom_trouve_dans_adresse DESC, taille_groupe_cp ASC
                   ) AS rn
            FROM irve_candidats_matches
        )
        WHERE rn = 1 AND (taille_groupe_cp = 1 OR nom_trouve_dans_adresse);
    """)

    # Vue finale : code_insee_commune d'origine complété par la reconstitution
    # quand elle a réussi, avec une colonne de traçabilité (jamais de
    # remplissage silencieux). puissance_nominale est aussi forcée en DOUBLE
    # ici (TRY_CAST défensif, pas CAST : une valeur qui ne parse pas devient
    # NULL plutôt que de faire planter tout le fichier -- voir
    # nb_puissance_non_castable dans le rapport pour repérer ces cas).
    con.execute("""
        CREATE OR REPLACE VIEW irve_avec_code_commune_trace AS
        SELECT
            b.* EXCLUDE (code_insee_commune, puissance_nominale),
            COALESCE(NULLIF(b.code_insee_commune, ''), rec.code_commune_reconstitue) AS code_insee_commune,
            TRY_CAST(b.puissance_nominale AS DOUBLE) AS puissance_nominale,
            CASE
                WHEN b.code_insee_commune IS NOT NULL AND b.code_insee_commune <> '' THEN 'brut'
                WHEN rec.code_commune_reconstitue IS NOT NULL THEN rec.methode
                ELSE 'non_resolu_probable_hors_france'
            END AS code_commune_origine
        FROM irve_brut b
        LEFT JOIN irve_code_commune_reconstitue rec ON rec.id_pdc_itinerance = b.id_pdc_itinerance;
    """)


def build_dedup_view(con: duckdb.DuckDBPyConnection) -> None:
    """Déduplique les points de charge (plusieurs versions d'un même pdc dans
    le flux consolidé) en gardant la version la plus récente.

    Deux pièges découverts sur le vrai fichier (2026-08-17, 224 267 lignes) :
      1. `id_pdc_itinerance` contient parfois le placeholder texte
         "Non concerné" (118 lignes) au lieu d'un vrai identifiant -- un
         simple `PARTITION BY id_pdc_itinerance` les collapse TOUTES en une
         seule ligne, perdant silencieusement 117 points de charge distincts.
         Fallback : `id_pdc_local` (souvent renseigné, ex: UUID opérateur)
         quand `id_pdc_itinerance` est ce placeholder ; si aucun des deux
         n'est exploitable (25 lignes sur les 118), la ligne garde une clé
         unique -- on ne la fusionne jamais avec une autre par défaut, faute
         de preuve que c'est un doublon (principe déjà appliqué ailleurs dans
         le projet : ne jamais perdre de donnée silencieusement).
      2. `date_maj` seul ne départage pas 5 284 groupes de pdc (plusieurs
         versions avec la même date_maj). `last_modified` (timestamp complet)
         les départage tous -- ajouté comme critère de tri secondaire.
    """
    con.execute("""
        CREATE OR REPLACE VIEW irve_avec_cle_dedup AS
        SELECT
            *,
            COALESCE(
                NULLIF(
                    CASE WHEN lower(trim(id_pdc_itinerance)) = 'non concerné' THEN NULL
                         ELSE NULLIF(trim(id_pdc_itinerance), '') END,
                    ''
                ),
                NULLIF('LOCAL::' || id_pdc_local, 'LOCAL::'),
                'ROW::' || ROW_NUMBER() OVER ()
            ) AS cle_dedup_pdc
        FROM irve_avec_code_commune_trace;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW irve_dedup AS
        SELECT * EXCLUDE (rn, cle_dedup_pdc)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cle_dedup_pdc
                       ORDER BY date_maj DESC NULLS LAST, last_modified DESC NULLS LAST
                   ) AS rn
            FROM irve_avec_cle_dedup
        )
        WHERE rn = 1;
    """)


@dataclass
class DedupReport:
    nb_lignes_avant_dedup: int
    nb_lignes_apres_dedup: int
    nb_doublons_supprimes: int
    # id_pdc_itinerance manquant ou égal au placeholder "Non concerné" -> clé
    # de secours (id_pdc_local ou ligne unique) utilisée pour ne pas fusionner
    # à tort des points de charge distincts.
    nb_pdc_id_non_fiable: int


def compute_dedup_report(con: duckdb.DuckDBPyConnection) -> DedupReport:
    avant, non_fiable = con.execute("""
        SELECT
            COUNT(*),
            SUM(CASE WHEN id_pdc_itinerance IS NULL OR trim(id_pdc_itinerance) = ''
                       OR lower(trim(id_pdc_itinerance)) = 'non concerné' THEN 1 ELSE 0 END)
        FROM irve_avec_cle_dedup
    """).fetchone()
    apres = con.execute("SELECT COUNT(*) FROM irve_dedup").fetchone()[0]
    return DedupReport(
        nb_lignes_avant_dedup=avant,
        nb_lignes_apres_dedup=apres,
        nb_doublons_supprimes=avant - apres,
        nb_pdc_id_non_fiable=non_fiable,
    )


@dataclass
class ReconstitutionReport:
    nb_pdc_total: int
    nb_pdc_code_brut: int
    nb_pdc_reconstitue_cp_unique: int
    nb_pdc_reconstitue_cp_nom: int
    nb_pdc_non_resolu: int
    # Lignes où puissance_nominale n'était pas vide dans le brut mais ne parse
    # pas en nombre (TRY_CAST -> NULL) -- 0 si tout est propre, signal de
    # dérive de format sinon (virgule décimale, texte, etc.).
    nb_puissance_non_castable: int


def compute_reconstitution_report(con: duckdb.DuckDBPyConnection) -> ReconstitutionReport:
    row = con.execute("""
        SELECT
            COUNT(*),
            SUM(CASE WHEN code_commune_origine = 'brut' THEN 1 ELSE 0 END),
            SUM(CASE WHEN code_commune_origine = 'cp_unique' THEN 1 ELSE 0 END),
            SUM(CASE WHEN code_commune_origine = 'cp_nom_trouve_dans_adresse' THEN 1 ELSE 0 END),
            SUM(CASE WHEN code_commune_origine = 'non_resolu_probable_hors_france' THEN 1 ELSE 0 END)
        FROM irve_avec_code_commune_trace
    """).fetchone()

    nb_puissance_non_castable = con.execute("""
        SELECT COUNT(*)
        FROM irve_brut
        WHERE puissance_nominale IS NOT NULL
          AND TRIM(puissance_nominale) <> ''
          AND TRY_CAST(puissance_nominale AS DOUBLE) IS NULL
    """).fetchone()[0]

    return ReconstitutionReport(*row, nb_puissance_non_castable)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Cleaning IRVE : reconstitution des code_insee_commune manquants via adresse_station"
    )
    parser.add_argument("--bucket", required=True, help="Bucket S3 (même bucket que la landing zone raw/)")
    parser.add_argument("--dt", required=True, help="Partition à nettoyer, format YYYY-MM-DD (doit exister sous raw/irve/dt=.../)")
    parser.add_argument("--region", default="eu-west-3")
    parser.add_argument(
        "--ref-codes-postaux",
        required=True,
        help="Chemin local vers 'Base officielle des codes postaux' (La Poste, CSV ';'), "
        "à télécharger sur https://www.data.gouv.fr/api/1/datasets/r/008a2dda-2c60-4b63-b910-998f6f818089",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Chemin S3 de sortie (CSV). Par défaut : s3://<bucket>/cleaned/irve/dt=<dt>/irve_consolide.csv",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

    irve_path = f"s3://{args.bucket}/raw/irve/dt={args.dt}/irve_consolide.csv"
    out = args.out or f"s3://{args.bucket}/cleaned/irve/dt={args.dt}/irve_consolide.csv"

    con = duckdb.connect()
    configure_s3(con, args.region)
    build_reconstitution_views(con, irve_path, args.ref_codes_postaux)
    build_dedup_view(con)

    report = compute_reconstitution_report(con)
    logger.info("Rapport de reconstitution (%s) :", irve_path)
    logger.info("  Total pdc : %s", report.nb_pdc_total)
    logger.info("  Déjà renseigné (brut) : %s", report.nb_pdc_code_brut)
    logger.info("  Reconstitué (CP unique) : %s", report.nb_pdc_reconstitue_cp_unique)
    logger.info("  Reconstitué (CP + nom trouvé dans l'adresse) : %s", report.nb_pdc_reconstitue_cp_nom)
    logger.info("  Non résolu (probablement hors France ou adresse illisible) : %s", report.nb_pdc_non_resolu)
    if report.nb_puissance_non_castable:
        logger.warning(
            "  %s valeur(s) de puissance_nominale non numériques (devenues NULL après TRY_CAST) !",
            report.nb_puissance_non_castable,
        )
    else:
        logger.info("  puissance_nominale : toutes les valeurs non vides sont bien numériques.")

    dedup_report = compute_dedup_report(con)
    logger.info("Rapport de dédoublonnage des points de charge :")
    logger.info("  Lignes avant dédup : %s", dedup_report.nb_lignes_avant_dedup)
    logger.info("  Lignes après dédup : %s", dedup_report.nb_lignes_apres_dedup)
    logger.info("  Doublons supprimés : %s", dedup_report.nb_doublons_supprimes)
    if dedup_report.nb_pdc_id_non_fiable:
        logger.warning(
            "  %s ligne(s) avec id_pdc_itinerance manquant/placeholder (repli sur id_pdc_local ou ligne unique) !",
            dedup_report.nb_pdc_id_non_fiable,
        )

    con.execute(f"COPY (SELECT * FROM irve_dedup) TO '{out}' (FORMAT CSV, HEADER true);")
    logger.info("Fichier IRVE nettoyé (dédupliqué) écrit -> %s", out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
