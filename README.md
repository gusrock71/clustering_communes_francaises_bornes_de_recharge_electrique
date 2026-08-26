# Pipeline VE — Brique d'ingestion raw (sourcing → S3)

Brique J1 du pipeline : télécharge les 3 sources brutes et les dépose telles
quelles (sans transformation) dans une landing zone S3, avec tests de bon
fonctionnement.

## Sources

| Source | Description | Producteur | Périodicité de mise à jour |
|---|---|---|---|
| `irve` | Base nationale consolidée des IRVE (bornes de recharge) | data.gouv.fr / transport.data.gouv.fr | **Quotidienne** (nouvelle consolidation chaque jour, confirmé le 2026-08-26 via l'historique des ressources) |
| `immatriculations` | Immatriculations de véhicules routiers par commune, par motorisation (2010-2025) — scindé en 2 fichiers SDES : `immatriculations_neuf` (achats neufs) et `immatriculations_occasion` (achats d'occasion, avec vignette Crit'Air) | SDES / data.gouv.fr | **Annuelle** (2 fichiers CSV mis à jour le 11/02/2026, confirmé le 2026-08-26) |
| `enedis_conso` | Consommation et thermosensibilité électriques annuelles par commune, **limité à l'année 2024** | Enedis Open Data (portail data-fair) | **Annuelle** (fréquence indiquée explicitement sur la fiche du dataset ; couverture 2011-2024, données publiées début 2025) |

URLs exactes et colonnes de garde-fou (`schema_hint`) : voir `ve_pipeline/ingestion/config.py`.

**Périodicité et implications pour l'automatisation (vérifié le 2026-08-26)** : l'IRVE est la seule source à évoluer quotidiennement, les 3 autres fichiers (immatriculations neuf/occasion, Enedis) ne changent qu'une fois par an. Un run mensuel ou trimestriel du connecteur d'ingestion suffit donc largement pour rester à jour sur l'ensemble des 4 sources -- pas besoin d'un rythme quotidien côté pipeline, même pour l'IRVE (les besoins de ce MVP portent sur des tendances de fond, pas sur du temps réel).

**Point de vigilance** : le filtre Enedis est figé sur `qs=annee:2024` dans `ve_pipeline/ingestion/config.py` (voir section dédiée ci-dessous). Une source annuelle publie généralement sa nouvelle année plusieurs mois après la clôture de l'année de référence (l'édition 2024 a été mise en ligne début 2025) -- il faut donc mettre à jour ce filtre manuellement une fois l'édition 2025 disponible sur le portail Enedis. La détection de ce moment n'est plus manuelle : voir `ve_pipeline/ingestion/freshness_cli.py` ci-dessous.

**Vérification de fraîcheur automatique du filtre Enedis (2026-08-26)** : `ve_pipeline/ingestion/freshness.py` compare l'année actuellement figée dans `config.py` à la dernière année réellement couverte par le dataset Enedis. Lit la page catalogue publique (`opendata.enedis.fr/datasets/...`) plutôt que l'API data-fair elle-même : l'endpoint `/data-fair/api/v1/datasets/.../lines` (celui utilisé par le connecteur pour le téléchargement réel) s'est révélé injoignable depuis l'environnement d'assistance utilisé pour écrire ce module, alors que la page catalogue publique répondait normalement -- même type de restriction réseau déjà rencontrée sur data.gouv.fr/data.enedis.fr (cf. section centroïdes communes). La page affiche une "Couverture temporelle" explicite (ex: "1 janvier 2011 - 31 décembre 2024") dont on extrait l'année de fin, sans avoir à paginer les 3,47M lignes du dataset complet juste pour connaître sa dernière année.

```bash
python -m ve_pipeline.ingestion.freshness_cli
```

Codes de sortie : `0` filtre à jour, `1` nouvelle année disponible (action requise dans `config.py`, jamais de mise à jour automatique du filtre -- décision produit à confirmer par l'utilisateur), `2` vérification impossible (page injoignable/format non reconnu, distinct de `1` pour ne pas déclencher une fausse alerte). Pensé pour tourner seul (cron, tâche planifiée) sans orchestrateur, cohérent avec la décision J4 du projet. Tests (`tests/test_ingestion_freshness.py`, 9 tests, HTTP mocké via `responses`) : extraction de l'année sur une page catalogue simulée, détection stale/à jour, échec explicite si le format de page n'est pas reconnu ou si la page est injoignable. Suite complète du projet : 108 passed / 3 skipped.

**Limite assumée** (documentée dans le module) : ce script dépend du texte affiché sur une page HTML publique, pas d'un contrat d'API stable -- si Enedis reformule significativement "Couverture temporelle", l'extraction échoue explicitement (`EnedisFreshnessCheckError`) plutôt que de renvoyer une année incorrecte en silence. **Vérification réelle non faite** : la page catalogue Enedis était injoignable depuis le sandbox d'assistance au moment d'écrire ce module (même restriction réseau que d'habitude) -- l'utilisateur doit lancer `python -m ve_pipeline.ingestion.freshness_cli` une première fois sur sa machine pour confirmer que le format réel de la page est bien reconnu.

### Cas particulier Enedis : source paginée

Contrairement à IRVE et immatriculations (un fichier CSV = un GET), Enedis
tourne sur une plateforme différente (data-fair, pas Opendatasoft) et le
dataset complet fait 3,47M lignes sur 2011-2024. L'API pagine par lots de
10 000 lignes via un header HTTP `Link: rel="next"`. Le connecteur :

1. suit les pages jusqu'à épuisement (`connector._download_paginated`),
2. recolle le CSV en ne gardant l'en-tête que de la première page,
3. filtre côté serveur sur l'année 2024 via `qs=annee:2024` (syntaxe Lucene standard data-fair, **confirmé en conditions réelles le 2026-08-16** : run complet réussi, 30 pages, 66 Mo),
4. vérifie après coup que toutes les lignes reçues ont bien `annee=2024` (`value_filter`) — si le filtre serveur n'a pas fonctionné, l'ingestion échoue explicitement au lieu de déposer des données hors périmètre.

Un garde-fou `max_pages=80` coupe court si jamais le filtre échoue et que la pagination part sur l'historique complet (~347 pages).

## Structure

```
ve_pipeline/ingestion/
  config.py       # déclaration des sources et de leurs fichiers
  connector.py    # téléchargement -> validation technique -> dépôt S3
  s3_landing.py   # wrapper boto3 (bucket, clé, upload, lecture)
  cli.py          # point d'entrée en ligne de commande
tests/
  test_ingestion_mocked.py  # tests hors-ligne (HTTP + S3 mockés) — exécutés ici
  test_ingestion_live.py    # tests contre les vraies sources — à lancer sur ta machine
scripts/
  demo_local_run.py         # démo de bout en bout, sans réseau ni AWS
```

Convention de nommage dans la landing zone :
```
s3://<bucket>/raw/<source>/dt=<YYYY-MM-DD>/<file_key>.<ext>
```

## ⚠️ Limite de l'environnement de développement

Cet environnement (sandbox) n'a **pas d'accès réseau sortant** vers
`data.gouv.fr` / `data.enedis.fr` (bloqué par allowlist). Les tests
`test_ingestion_mocked.py` (9/9 ✅) ont donc validé toute la logique
(téléchargement, garde-fous, upload S3, intégrité, idempotence, gestion
d'erreurs) avec des sources et un S3 simulés — mais **pas encore la
connectivité réelle**.

À faire sur ta machine (ou en CI) avant de considérer le J1 vraiment
terminé :

```bash
pip install -r requirements-dev.txt

# 1. Test hors-ligne (déjà validé ici, doit repasser au vert chez toi aussi)
pytest tests/test_ingestion_mocked.py -v

# 2. Test de bon fonctionnement contre les VRAIES sources (réseau requis)
RUN_LIVE_TESTS=1 pytest tests/test_ingestion_live.py -v -m live

# 3. Run réel vers un bucket S3 (remplace <bucket> par le tien)
python -m ve_pipeline.ingestion.cli --bucket <bucket> --source all
```

Le test live utilise par défaut un bucket S3 mocké (moto) pour ne valider
que la connectivité aux sources. Pour tester aussi un vrai bucket AWS,
positionne `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`VE_PIPELINE_TEST_BUCKET` et `VE_PIPELINE_USE_REAL_S3=1`.

**Point d'attention pour le premier run réel** : les `schema_hint` du
fichier `immatriculations` (SDES) sont vides dans `config.py` faute d'avoir
pu inspecter l'en-tête réel depuis le sandbox — à compléter dès le premier
téléchargement réussi pour activer le garde-fou anti-dérive de schéma sur
cette source aussi.

## Démo sans réseau ni AWS

```bash
python3 scripts/demo_local_run.py
```

Simule les 3 sources et un bucket S3, exécute le pipeline complet, affiche
le résultat JSON par fichier et le contenu de la landing zone.

## Chargement S3 → Postgres (staging)

`ve_pipeline/loading/postgres_loader.py` charge les fichiers raw S3 dans une
base Postgres (Neon) — c'est le point d'entrée de tout ce qui vient après
(dbt, voir plus bas). Le loader est agnostique de l'origine des fichiers
(Airbyte ou connecteur Python Enedis) : il ne connaît que la convention de
nommage S3 déjà en place.

Contrainte d'infra (quota gratuit Neon, 512 Mo/projet) : `postgres_loader.py`
n'insère pas toutes les colonnes brutes tel quel pour IRVE/Enedis/
immatriculations -- voir `DROPPED_COLUMNS`/`KEPT_COLUMNS` dans ce module pour
le détail et la justification de chaque exclusion (le fichier S3 correspondant,
lui, reste toujours intact).

```bash
# Sans DATABASE_URL : charge dans un SQLite local (ve_pipeline_staging.db)
python -m ve_pipeline.loading.cli --bucket <bucket> --source all

# Contre un vrai Postgres (Neon/Supabase)
DATABASE_URL=postgresql+psycopg2://user:pass@host/db \
    python -m ve_pipeline.loading.cli --bucket <bucket> --source all

# Forcer une partition dt= précise (sinon : la plus récente par source)
python -m ve_pipeline.loading.cli --bucket <bucket> --source all --dt 2026-08-16
```

**Partition chargée par défaut** : sans `--dt`, le loader ne suppose PAS que
l'ingestion a eu lieu aujourd'hui — il recherche sur S3 la partition `dt=`
la plus récente disponible pour chaque source (`s3_landing.latest_partition`,
même logique que `scripts/explore_s3_pyspark.py`). Ingestion (J1) et
chargement peuvent donc tourner des jours différents sans provoquer de
`NoSuchKey`. `--dt` force une date précise si besoin (rejouer un jour
donné).

Chaque fichier atterrit dans une table `raw__<source>__<file_key>` (colonnes
en texte, noms normalisés en snake_case ASCII — ex. "Code Commune" devient
`code_commune`), sans renommage ni typage métier : ce sera le rôle de dbt.
Le nombre de lignes chargées est vérifié contre le fichier source (échec
explicite sinon). Le délimiteur CSV (`;` pour IRVE/immatriculations, `,`
pour Enedis) est détecté automatiquement plutôt que codé en dur. Testé avec
SQLite comme stand-in de Postgres (même code SQLAlchemy des deux côtés) :
`pytest tests/test_postgres_loader.py -v`.

## Transformation : dbt (chemin principal depuis le 2026-08-21)

La jointure territoriale (J2) et la préparation du dataset clustering (J3)
tournent désormais en dbt, sur les tables Postgres/Neon chargées par
`postgres_loader.py` -- plus en DuckDB directement sur S3.

```
dbt/
  dbt_project.yml
  profiles.yml            # à copier vers ~/.dbt/profiles.yml, ou --profiles-dir .
  macros/
    safe_cast.sql          # équivalent du TRY_CAST DuckDB (Postgres n'en a pas nativement)
    generate_schema_name.sql
  seeds/
    ref_codes_postaux.csv     # La Poste, "Base officielle des codes postaux" (reconverti UTF-8)
    ref_densite_communes.csv  # Grille de densité Insee 2026 -- code_commune, zone_densite (urbain/rural)
  models/
    staging/                               # stg_irve, stg_immatriculations_{neuf,occasion}, stg_enedis
    intermediate/int_irve_cleaned.sql        # port de ve_pipeline/cleaning/irve_code_commune.py
    intermediate/int_territorial_join.sql    # port de ve_pipeline/jointure/build_staging.py
    marts/mart_clustering_dataset.sql        # port de ve_pipeline/features/build_clustering_dataset.py
```

`dbt seed` doit être lancé au moins une fois (charge `ref_codes_postaux`
avant que `int_irve_cleaned` puisse tourner) : `dbt seed --profiles-dir .`.

Connexion : dbt-postgres n'accepte pas un DSN unique -- positionner ces
variables d'environnement (mêmes valeurs que ton `DATABASE_URL` actuel,
consultables dans la console Neon) avant de lancer dbt :
`DBT_NEON_HOST`, `DBT_NEON_USER`, `DBT_NEON_PASSWORD`, `DBT_NEON_DBNAME`
(`DBT_NEON_PORT`, optionnel, défaut 5432).

```bash
pip install -r requirements-dbt.txt
cd dbt
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

**Validation effectuée** (le 2026-08-21, cette session n'a pas d'accès réseau
sortant vers Neon pour lancer `dbt run` elle-même) : la logique SQL de
`int_irve_cleaned`, `int_territorial_join` et `mart_clustering_dataset` a
été exécutée directement contre une vraie base Postgres (branche Neon
temporaire, `br-still-glade-b1oiiame`, supprimée après coup) avec les mêmes
jeux de données que `tests/test_jointure.py`,
`tests/test_cleaning_irve_code_commune.py` et
`tests/test_features_clustering_dataset.py` -- résultats identiques à ceux
déjà validés côté DuckDB (dédoublonnage, reconstitution des codes commune
via `unaccent`/`regexp_matches`, jointure, imputation médiane). `dbt parse`
a aussi été exécuté avec succès (validation Jinja/config). Le `dbt run`/
`dbt test`/`dbt seed` complet reste à lancer une fois sur ta machine pour
confirmer de bout en bout (schémas créés, matérialisation, tests dbt).

**Écart connu par rapport au pipeline DuckDB/S3** (décision prise
explicitement avec l'utilisateur le 2026-08-21) : les immatriculations
n'exposent que les années 2018-2025 en Postgres (`IMMAT_2010`...`IMMAT_2017`
exclus pour tenir dans le quota gratuit Neon) -- `int_territorial_join`
n'expose donc le détail année par année que sur cette plage, pas 2010-2025.

**Filtre véhicules concernés par la recharge IRVE** (décision du
2026-08-21, dbt uniquement -- le pipeline DuckDB/S3 en sauvegarde n'a pas
été modifié) : `stg_immatriculations_neuf`/`stg_immatriculations_occasion`
filtrent désormais sur `groupe`/`categorie` pour ne garder que les
véhicules qui rechargent réellement sur le réseau de bornes IRVE public :
`VP`, `VUL`, et `CATL`/`MOTOCYCLETTE` (la plupart des motos électriques ont
un connecteur Type 2 et se rechargent comme une voiture sur les mêmes
bornes AC 6-22 kW). Exclus : `CATL`/`CYCLOMOTEUR` et `CATL`/`VOITURETTE`
(batterie amovible ou recharge native sur prise domestique -- Citroën Ami
par ex., impact négligeable sur le dimensionnement capacitaire), ainsi que
`AUTRES` (remorque, tracteur agricole), `PL` (camion, tracteur routier) et
`TCP` (bus, autocar), qui nécessitent une infrastructure de recharge dédiée
(mégawatt, dépôt) hors périmètre MVP. Vérifié sur les données réelles Neon
le 2026-08-21 : le filtre conserve 598 492/934 023 lignes (64%) côté
`immatriculations_neuf` et 1 852 248/2 607 726 lignes (71%) côté
`immatriculations_occasion`.

**Zone de densité (urbain/rural)** (ajouté le 2026-08-21) : `int_territorial_join`
expose désormais `zone_densite` ('urbain'/'rural'), issue de la grille de
densité Insee (millésime 2026, géographie au 01/01/2026, RP 2021 --
https://www.insee.fr/fr/information/8571524). Seule la colonne DENS (3
postes : Urbain dense / Urbain intermédiaire / Rural) est reprise, en
regroupant les deux premières en 'urbain' -- `dbt/seeds/ref_densite_communes.csv`
(extrait depuis le fichier officiel, 34 875 communes, aucun doublon de
code, DOM et Corse inclus). C'est une variable de contexte territorial
(pas utilisée dans les agrégations), propagée jusqu'à `mart_clustering_dataset`
pour interprétation post-hoc des clusters K-Means. Jointure validée sur une
branche Neon temporaire (`br-empty-cloud-b1si94pg`, supprimée après coup) :
100% de correspondance sur l'échantillon testé (communes du département 01).

**Features de couverture VE/réseau pour le clustering** (ajouté le
2026-08-21) : `mart_clustering_dataset` expose désormais `nb_ve_stock_estime`
(somme `immat_ve_2018`...`immat_ve_2025`, proxy du parc VE en circulation --
un flux cumulé, pas un vrai stock), `taux_couverture_afir` (puissance
installée / (1,3 kW × stock VE), 1,3 kW = seuil réglementaire AFIR) et
`ratio_pdc_par_100_ve` (nb PDC / stock VE, normalisé par le benchmark
national Insee de la zone -- 0,24 urbain / 0,10 rural, variables dbt
`benchmark_pdc_urbain`/`benchmark_pdc_rural` dans `dbt_project.yml`).
`zone_densite` sert uniquement à ce calcul (benchmark + médiane
d'imputation par zone) et n'est volontairement pas repris comme colonne de
sortie du mart, pour éviter un doublon avec l'information déjà absorbée
dans le ratio. Stock VE = 0 -> ratio imputé par la médiane de la même zone
(repli sur la médiane globale si la zone n'a aucune valeur calculable) ;
stock non nul mais faible -> ratio plafonné à 3x le repère (`plafond_ratio_couverture`)
plutôt qu'imputé, pour éviter qu'une poignée de communes minuscules
n'écrase les distances du K-Means. Logique validée par un test synthétique
sur une branche Neon temporaire (`br-silent-star-b1gn3fqp`, supprimée après
coup) : premier passage détecté un bug réel (la médiane d'imputation
calculée sur les valeurs brutes non plafonnées pouvait elle-même dépasser
le plafond) -- corrigé en calculant la médiane sur les valeurs déjà
plafonnées (`least(brut, plafond)`), revalidé ensuite avec succès (toutes
les valeurs finales bornées à [0, plafond]).

## Clustering : K-Means + MLflow (2026-08-22)

`ve_pipeline/clustering/` entraîne le modèle K-Means sur `marts.mart_clustering_dataset`
(Postgres/Neon) et trace l'expérimentation dans MLflow.

```
ve_pipeline/clustering/
  kmeans_model.py   # chargement des features, pipeline StandardScaler+KMeans,
                     # sélection de k, tracking MLflow, écriture des assignations
  cli.py             # point d'entrée en ligne de commande
```

Features retenues (décision du 2026-08-22, discussion avec l'utilisateur) :
`part_thermosensible`, `taux_chauffage_electrique` (risque de tension
réseau), `croissance_immat_ve_pct`, `demarrage_ve_tardif` (dynamique
d'adoption VE), `taux_couverture_afir`, `ratio_pdc_par_100_ve` (équipement
en bornes relatif au besoin). `zone_densite` n'est volontairement pas
incluse (déjà absorbée dans `ratio_pdc_par_100_ve`).

k est sélectionné parmi {3, 4} par score de silhouette (décision produit :
3-4 profils maximum pour rester actionnable côté gestionnaire/collectivité)
-- surchageable via `--k`. Chaque candidat testé est loggé comme run MLflow
distinct (`selected=False`), plus un run final (`selected=True`) portant
l'artefact modèle (pipeline complet StandardScaler+KMeans, réutilisable tel
quel pour scorer de nouvelles communes) et le profil moyen par cluster
(CSV, pour l'interprétation/le nommage des clusters).

```bash
pip install -r requirements.txt

# Entraînement (sélection automatique de k parmi 3,4 par silhouette)
DATABASE_URL=postgresql+psycopg2://user:pass@host/db \
    python -m ve_pipeline.clustering.cli

# Forcer k=4 plutôt que la sélection automatique
python -m ve_pipeline.clustering.cli --k 4

# Consulter les runs (backend SQLite local par défaut, voir plus bas)
mlflow ui --backend-store-uri sqlite:///mlflow.db
```

Les assignations (`code_commune`, les 6 features, `cluster_id`,
`mlflow_run_id`, `trained_at`) sont écrites dans une table à plat
`ml__cluster_assignments` (même convention que `raw__<source>__<file>` --
pas de schéma SQL séparé, compatible SQLite en test comme Postgres en prod).
`if_exists='replace'` : cette table représente le DERNIER clustering
entraîné, pas un historique -- l'historique complet (tous les k testés,
métriques, modèle) reste dans MLflow.

**Découverte en testant ce module (2026-08-22, mlflow 3.15.1)** : le backend
de tracking fichier brut (`file:./mlruns`) est désormais refusé par défaut
par MLflow (`FileStore` en "maintenance mode", lève une exception à moins de
positionner `MLFLOW_ALLOW_FILE_STORE=true`). Le repli local par défaut de ce
module utilise donc un backend SQLite (`sqlite:///mlflow.db`,
`DEFAULT_TRACKING_URI` dans `kmeans_model.py`) plutôt que le `./mlruns` brut
-- toujours surchargeable via `--tracking-uri` ou `MLFLOW_TRACKING_URI`. Les
artefacts (modèle, profil CSV) restent stockés localement sous `./mlruns/`
(racine des artefacts, distincte du backend de tracking) -- ajouté au
`.gitignore` avec `mlflow.db`.

Tests (`tests/test_kmeans_model.py`, 7 tests) : dataset synthétique à 3
groupes réellement séparés sur les 6 features (vérifie que la silhouette
choisit bien k=3 plutôt que k=4), chargement/rejet des NULL inattendus,
tracking MLflow sur backend SQLite temporaire, écriture des assignations
(idempotence d'un ré-entraînement). Suite complète : 74 passed / 3 skipped.

## Explorations alternatives au clustering K-Means (2026-08-22)

Deux modules exploratoires, séparés du pipeline de production
(`kmeans_model.py`/`cli.py`, jamais remplacé tant qu'un résultat n'est pas
clairement meilleur) :

- `ve_pipeline/clustering/dbscan_explore.py` -- grille (`eps`, `min_samples`).
  Verdict sur données réelles : soit beaucoup de bruit (jusqu'à 23% des
  communes non classées) pour un score de silhouette à peine meilleur, soit
  seulement 2 clusters (le dégradé continu du groupe intermédiaire fusionne
  en un bloc). K-Means k=4 reste préférable. Détail sur Notion.
- `ve_pipeline/clustering/gmm_explore.py` -- grille (`n_components` ∈ {3,4},
  `covariance_type` ∈ {full, tied, diag, spherical}). Assignation
  probabiliste (chaque commune reçoit une probabilité d'appartenance par
  cluster, pas seulement une classe) plutôt que la frontière dure de
  K-Means -- pertinent ici puisqu'on a confirmé l'absence de séparation
  nette entre profils. Sélection du meilleur candidat par BIC (critère
  natif des mélanges gaussiens, contrairement à K-Means qui n'en a pas).

```bash
python -m ve_pipeline.clustering.dbscan_explore
python -m ve_pipeline.clustering.gmm_explore
```

Tests : `tests/test_dbscan_explore.py` (4 tests), `tests/test_gmm_explore.py`
(4 tests). Suite complète du projet : 82 passed / 3 skipped.

## API de service (FastAPI) + déploiement Cloud Run (2026-08-24)

Étape suivante décidée le 2026-08-24 : rattacher le modèle K-Means k=4
retenu à une future application Streamlit via une API. Stack choisie après
comparaison (Render/Railway/Cloud Run) : **Cloud Run**, cohérent avec
l'orientation GCP déjà envisagée pour ce projet.

**`ve_pipeline/api/main.py`** -- API FastAPI en lecture seule. Rôle
volontairement limité : elle sert les clusters déjà calculés et écrits par
`kmeans_model.write_cluster_assignments` (table `ml__cluster_assignments`),
sans jamais recharger le pipeline sklearn ni dépendre de MLflow en
production -- le modèle reste un outil d'entraînement/traçabilité, la mise
en service se fait sur le résultat déjà en base. Ce choix garde l'image
Docker légère (ni boto3, ni duckdb, ni pyspark, ni mlflow --
`requirements-api.txt` séparé de `requirements.txt`).

Endpoints :
- `GET /health`
- `GET /communes/search?q=...` -- recherche par nom (sous-chaîne), code
  postal ou code INSEE (match exact), jointure avec le seed dbt
  `staging.ref_codes_postaux` (La Poste) pour le nom/code postal.
- `GET /communes/map` -- jeu complet (une ligne par commune AVEC centroïde
  connu : code, nom, cluster, latitude, longitude), pensé pour alimenter la
  carte Streamlit. Part de `staging.ref_centroides_communes` (univers quasi
  complet, 34 969 communes) avec jointure optionnelle vers
  `ml__cluster_assignments` : les communes exclues du clustering faute de
  données immatriculations/Enedis suffisantes (~2 870 sur 34 969, filtre
  `has_immatriculations`/`has_enedis` de `mart_clustering_dataset.sql`)
  apparaissent quand même, avec `cluster_id = null` -- décision du
  2026-08-25 après constat de zones blanches inexpliquées sur la première
  version de la carte (voir section Streamlit ci-dessous). Seules les
  communes sans centroïde connu (aucune à ce jour) restent exclues.
- `GET /communes/{code_commune}` -- détail d'une commune (cluster +
  features).

**Limitation connue** : le référentiel La Poste ne liste pas Paris, Lyon,
Marseille sous leur code INSEE agrégé (75056, 69123, 13055) mais
arrondissement par arrondissement (ex. 75101...75120) -- une recherche par
nom ne les retrouve donc pas, et `/communes/75056` renvoie le cluster avec
`nom_commune`/`code_postal` à `null`. À enrichir plus tard (3 lignes à
ajouter manuellement) si besoin pour la version Streamlit.

Tests (`tests/test_api.py`, 11 tests, SQLite comme stand-in de Postgres, même
convention que le reste du projet) : recherche par code/nom, doublons
(commune à plusieurs codes postaux, ex. Paris/arrondissements), 404 sur
commune/recherche introuvable, `/communes/map` avec commune non clusterisée
(`cluster_id = null`) et commune sans centroïde connu (exclue). Suite
complète : 99 passed / 3 skipped.

**Déploiement Cloud Run** :
- Projet GCP dédié : `ve-clustering-api` (région `europe-west9`, Paris).
- `Dockerfile` (racine) : image `python:3.11-slim`, ne copie que
  `requirements-api.txt` + `ve_pipeline/api/` -- build rapide, image légère.
- `DATABASE_URL` stocké dans Secret Manager (secret `database-url`), jamais
  en clair dans l'image/les logs/la commande de déploiement. Accès accordé
  au compte de service Compute Engine par défaut du projet
  (`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`, rôle "Secret
  Manager Secret Accessor").
- Déploiement direct depuis le code source local (pas besoin de Docker
  installé en local, ni de GitHub) :
```bash
gcloud run deploy ve-clustering-api \
  --source . \
  --region europe-west9 \
  --allow-unauthenticated \
  --set-secrets=DATABASE_URL=database-url:latest
```
- `--allow-unauthenticated` : API publique sans authentification, nécessaire
  pour l'appel depuis Streamlit (hébergé séparément) -- acceptable ici, les
  données servies (clusters par commune) ne sont pas des données
  personnelles sensibles.
- Service déployé et testé en conditions réelles le 2026-08-24 :
  `https://ve-clustering-api-164992414488.europe-west9.run.app` (`/health`,
  `/communes/search`, `/communes/{code_commune}` tous vérifiés en production
  contre les 32 101 communes réelles).
- Free tier GCP largement suffisant pour ce projet (trafic quasi nul) :
  Cloud Run (2M requêtes/mois), Secret Manager (10k accès/mois), Cloud Build
  (120 min/jour), Artifact Registry (0,5 Go) -- seul point de vigilance à
  moyen terme : nettoyer les anciennes images Artifact Registry si
  redéploiements très fréquents.

## Référentiel centroïdes communes (2026-08-25)

Nouveau seed dbt `ref_centroides_communes` (`code_commune;latitude;longitude`,
schéma `staging`) : coordonnées de 34 969 communes, source `geo.api.gouv.fr`.

**Contrainte rencontrée** : l'outil de récupération web disponible dans
l'environnement d'assistance est plafonné à ~80 Ko par requête HTTP (constaté
en JSON comme en CSV) -- trop peu pour les ~35 000 communes en un seul appel.
Un découpage département par département (~100 requêtes) fonctionne mais
consomme trop de ressources conversationnelles pour être fait depuis cet
environnement. Solution retenue : script ponctuel `fetch_centroides.py`
(racine du projet, dépendances stdlib uniquement), exécuté directement sur la
machine de l'utilisateur -- un seul appel HTTP normal y suffit, sans la
limite de taille rencontrée côté outil d'assistance.

```bash
python3 fetch_centroides.py   # écrit dbt/seeds/ref_centroides_communes.csv
cd dbt && dbt seed --select ref_centroides_communes --profiles-dir .
```

Résultat : 34 969 communes, 0 sans centre connu, aucun doublon, codes Corse
(2A/2B) préservés.

## Application Streamlit (2026-08-25)

`streamlit_app/app.py` -- front-end de restitution, séparé de l'API comme
convenu (2026-08-24) : n'accède JAMAIS directement à Postgres/Neon, appelle
uniquement l'API HTTP (`API_BASE_URL`, par défaut le service Cloud Run
déployé). Deux fonctionnalités :
- recherche par nom, code postal ou code INSEE (`GET /communes/search`),
  affichage du cluster et des features clés par commune ;
- carte de l'ensemble des communes (`GET /communes/map`, pydeck
  `ScatterplotLayer`), un point coloré par cluster (4 couleurs fixes, k=4)
  pour repérer visuellement les grands ensembles (déserts IRVE, zones à
  risque réseau).

**Communes "données insuffisantes" (2026-08-25)** : la première version de
la carte affichait des zones blanches inexpliquées (32 101 communes
affichées sur 34 969 avec centroïde connu). Cause : `/communes/map`
n'incluait que les communes déjà présentes dans `ml__cluster_assignments`,
excluant silencieusement celles écartées du clustering faute de données
immatriculations/Enedis (~2 870 communes). Corrigé en modifiant l'endpoint
pour renvoyer `cluster_id = null` sur ces communes plutôt que de les omettre
(voir section API ci-dessus) ; côté Streamlit, `load_map_data()` colore ces
points en gris clair (`INSUFFICIENT_DATA_COLOR`, distinct du gris de
garde-fou `DEFAULT_COLOR`) avec une entrée de légende dédiée "Données
insuffisantes" et un libellé de tooltip adapté (`cluster_label`).

`render_app()` isole toute la construction de l'UI dans une fonction
appelée uniquement sous `if __name__ == "__main__"` -- Streamlit exécute
justement le script cible avec `__name__ == "__main__"`
(`streamlit run app.py`), donc ce garde-fou ne change rien à l'usage normal
mais rend `search_communes`/`CLUSTER_COLORS` testables sans dépendre d'un
contexte Streamlit actif.

```bash
pip install -r requirements-streamlit.txt
streamlit run streamlit_app/app.py
```

Tests (`tests/test_streamlit_app.py`, 6 tests, `responses` pour mocker
l'API) : recherche réussie, 404 traité comme "aucun résultat" (pas une
erreur), erreur serveur, erreur réseau, couverture des 4 clusters dans la
légende, coloration grise "données insuffisantes" pour `cluster_id = null`.
Suite complète du projet : 99 passed / 3 skipped.

Prochaine étape : déploiement sur Streamlit Community Cloud (gratuit,
connexion directe au repo GitHub).

## Sauvegarde : ancien pipeline DuckDB/S3

`ve_pipeline/jointure/`, `ve_pipeline/cleaning/`, `ve_pipeline/features/`
sont conservés dans le dépôt à la demande explicite de l'utilisateur, mais
ne sont plus le chemin de production (remplacés par `dbt/` ci-dessus). Voir
l'en-tête de chaque fichier pour le détail. Les tests associés (`tests/test_jointure.py`,
`tests/test_cleaning_irve_code_commune.py`, `tests/test_features_clustering_dataset.py`)
restent en place et servent aussi de jeu de référence pour valider le
portage dbt.
