"""Déclaration des 3 sources brutes du pipeline VE et de leurs fichiers.

Chaque source pointe vers une URL stable de type "resource redirect"
(data.gouv.fr) ou "export API" (Enedis Open Data / Opendatasoft), qui
renvoie toujours la dernière version publiée du fichier.

IMPORTANT — à vérifier lors du premier run réel (réseau sortant requis,
voir README) :
  - Le `schema_hint` de chaque fichier est une liste de sous-chaînes qui
    DOIVENT apparaître dans les ~4096 premiers octets du fichier (en-tête
    CSV). Cela sert de garde-fou anti-dérive de schéma : si la source
    change de format sans préavis, l'ingestion échoue explicitement au
    lieu de déposer silencieusement un fichier inexploitable dans la
    landing zone.
  - Les `schema_hint` ci-dessous pour "immatriculations" sont indicatifs
    (colonnes usuelles SDES : codgeo/epci/annee/...) et doivent être
    confirmés/ajustés dès que le fichier réel est téléchargé une première
    fois (le sandbox de développement n'a pas d'accès réseau sortant vers
    data.gouv.fr pour le vérifier automatiquement).
  - La source "enedis_conso" tourne sur une plateforme différente (data-fair,
    pas Opendatasoft) : le dataset fait 3,47M lignes et l'API pagine par lots
    de 10 000 lignes via un header HTTP `Link: rel="next"`, pas de fichier
    unique en un seul GET. Voir `paginated`/`value_filter` ci-dessous. URL,
    pagination, filtre année et schéma réel confirmés en conditions réelles
    le 2026-08-16 (run complet réussi : 30 pages, 66 Mo).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SourceFile:
    """Un fichier téléchargeable appartenant à une source."""

    key: str  # identifiant court, utilisé dans le nom d'objet S3
    url: str
    expected_ext: str  # csv, geojson...
    expected_content_type: tuple[str, ...] = (
        "text/csv",
        "application/csv",
        "text/plain",
        "application/octet-stream",
        "application/geo+json",
        "application/json",
    )
    schema_hint: tuple[str, ...] = field(default_factory=tuple)

    # Pagination façon data-fair (curseur via header HTTP `Link: rel="next"`).
    # Quand True, le connecteur suit les pages jusqu'à épuisement et recolle
    # le CSV (en-tête gardé une seule fois), au lieu d'un simple GET unique.
    paginated: bool = False
    max_pages: int = 1  # garde-fou anti-boucle infinie / dérive de volume si paginated=True

    # Vérification de filtre serveur : (sous-chaîne de nom de colonne, valeur
    # attendue). Après téléchargement, le connecteur vérifie que TOUTES les
    # lignes ont cette valeur dans la colonne correspondante — utile quand le
    # filtrage se fait via un paramètre de requête (ex: qs=annee:2024) dont on
    # ne peut pas garantir a priori la syntaxe exacte côté serveur : si le
    # filtre n'a pas été appliqué, l'ingestion échoue explicitement plutôt que
    # de déposer silencieusement des données hors périmètre.
    value_filter: tuple[str, str] | None = None


@dataclass(frozen=True)
class Source:
    name: str  # irve | immatriculations | enedis_conso
    description: str
    files: tuple[SourceFile, ...]


SOURCES: dict[str, Source] = {
    "irve": Source(
        name="irve",
        description=(
            "Base nationale consolidée des IRVE (bornes de recharge) — "
            "data.gouv.fr / transport.data.gouv.fr, schéma etalab/schema-irve-statique"
        ),
        files=(
            SourceFile(
                key="irve_consolide",
                url="https://www.data.gouv.fr/api/1/datasets/r/eb76d20a-8501-400e-b336-d85724de5435",
                expected_ext="csv",
                schema_hint=("id_pdc_itinerance", "code_insee_commune", "puissance_nominale"),
            ),
        ),
    ),
    "immatriculations": Source(
        name="immatriculations",
        description=(
            "Immatriculations de véhicules routiers par commune, par motorisation "
            "(2010-2025) — SDES / data.gouv.fr. Le dataset SDES est scindé en 2 fichiers "
            "distincts par marché (neuf / occasion), voir le détail de chaque SourceFile."
        ),
        files=(
            SourceFile(
                # "Immatriculations de véhicules achetés neufs au niveau communal" :
                # opérations d'immatriculation classiques + véhicules de démonstration
                # des concessionnaires, comptés parmi les neufs.
                key="immatriculations_neuf",
                url="https://www.data.gouv.fr/api/1/datasets/r/b2bac57a-7a25-4df1-9808-46b96b25311d",
                expected_ext="csv",
                schema_hint=(),  # colonnes exactes à confirmer au premier run réel
            ),
            SourceFile(
                # "Immatriculations de véhicules achetés d'occasion au niveau communal" :
                # changements de titulaire, 1ères immatriculations d'occasion importées,
                # changements de locataire longue durée + catégorie Crit'Air.
                key="immatriculations_occasion",
                url="https://www.data.gouv.fr/api/1/datasets/r/eac49e71-bad2-49a0-980f-59bc960f5e2c",
                expected_ext="csv",
                schema_hint=(),  # colonnes exactes à confirmer au premier run réel
            ),
        ),
    ),
    "enedis_conso": Source(
        name="enedis_conso",
        description=(
            "Consommation et thermosensibilité électriques annuelles par secteur "
            "d'activité à la maille commune, limité à l'année 2024 — Enedis Open Data "
            "(portail data-fair, paginé par lots de 10 000 lignes)"
        ),
        files=(
            SourceFile(
                # URL et pagination confirmées en capturant la requête réelle de
                # l'UI (onglet Réseau du navigateur) le 2026-08-16 : endpoint
                # /data-fair/api/v1/datasets/<slug>/lines, format=csv, size max
                # 10000/page, pagination suivante via le header `Link: rel="next"`
                # de la réponse (pas besoin de reconstruire l'URL nous-mêmes).
                #
                # Le filtre `qs=annee:2024` (syntaxe Lucene, standard data-fair)
                # a été confirmé en conditions réelles le 2026-08-16 (run complet :
                # 30 pages, 66 Mo, pagination terminée proprement).
                #
                # En-tête réel du CSV (confirmé le 2026-08-16, avec BOM UTF-8 en
                # tête — géré via decode "utf-8-sig" dans connector.py) :
                #   "Année","Code Commune","Nom Commune","Code EPCI","Nom EPCI",
                #   "Type EPCI","Code Département", ..., "nb_sites",
                #   "Conso totale (MWh)","Conso moyenne (MWh)", ...,
                #   "nombre_d_habitants", ...
                # Attention : les noms de colonnes ne sont pas en snake_case pour
                # la plupart (accents, espaces, majuscules) — à reprendre tel
                # quel lors de la jointure J2, pas de renommage automatique ici
                # (principe "raw, sans transformation").
                key="enedis_conso_commune_2024",
                url=(
                    "https://opendata.enedis.fr/data-fair/api/v1/datasets/"
                    "consommation-electrique-par-secteur-dactivite-commune/lines"
                    "?draft=false&size=10000&page=1&format=csv&sep=,&qs=annee:2024"
                ),
                expected_ext="csv",
                schema_hint=("année", "code commune"),
                paginated=True,
                # Confirmé en conditions réelles : 30 pages pour 2024. Plafond à
                # 80 conservé comme garde-fou (si le filtre `qs` cesse de
                # fonctionner un jour, ça coupe avant de repartir sur les 347
                # pages de l'historique complet).
                max_pages=80,
                value_filter=("année", "2024"),
            ),
        ),
    ),
}
