-- Port dbt/Postgres de ve_pipeline/cleaning/irve_code_commune.py (gardé en
-- sauvegarde DuckDB/S3). Deux étapes, dans l'ordre :
--   1. Reconstitution des code_insee_commune manquants à partir de
--      l'adresse libre (candidats code postal extraits par regex, résolus
--      via le référentiel La Poste -- seed dbt {{ ref('ref_codes_postaux') }}).
--   2. Dédoublonnage des points de charge (plusieurs versions du même pdc
--      dans le flux consolidé), même clé de repli que côté DuckDB :
--      id_pdc_itinerance, sauf placeholder "Non concerné" -> id_pdc_local,
--      sinon clé unique par ligne (jamais fusionné par défaut).
--
-- Différences techniques DuckDB -> Postgres :
--   - regexp_extract_all(...) + unnest(...) -> regexp_matches(..., 'g') en
--     LATERAL JOIN (même effet : une ligne par candidat trouvé).
--   - `\b` (frontière de mot, PCRE) -> `\y` (dialecte POSIX de Postgres).
--   - strip_accents(...) -> unaccent(...) (extension Postgres standard,
--     activée via le hook on-run-start du dbt_project.yml).
--   - puissance_nominale : déjà castée en amont dans stg_irve (safe_cast),
--     pas besoin de la recaster ici.
--
-- Non repris ici : le "rapport" (compute_reconstitution_report /
-- compute_dedup_report côté DuckDB) est un résumé de logs, pas une
-- transformation -- à faire via une requête SQL ponctuelle si besoin plutôt
-- que matérialisé en modèle.

with irve_brut as (

    select * from {{ ref('stg_irve') }}

),

candidats_cp as (

    select
        b.id_pdc_itinerance,
        b.adresse_station,
        upper(unaccent(b.adresse_station)) as adresse_normalisee,
        m.match[1] as cp_candidat
    from irve_brut b
    cross join lateral regexp_matches(b.adresse_station, '\y\d{4,5}\y', 'g') as m(match)
    where b.code_insee_commune is null or b.code_insee_commune = ''

),

candidats_matches as (

    select
        c.id_pdc_itinerance,
        r.code_commune_insee,
        r.nom_de_la_commune,
        r.code_postal,
        count(*) over (partition by c.id_pdc_itinerance, r.code_postal) as taille_groupe_cp,
        (position(upper(unaccent(r.nom_de_la_commune)) in c.adresse_normalisee) > 0) as nom_trouve_dans_adresse
    from candidats_cp c
    join {{ ref('ref_codes_postaux') }} r on r.code_postal = c.cp_candidat

),

-- Sélectionne le meilleur candidat par pdc : priorité au nom trouvé dans
-- l'adresse, puis au groupe le plus petit (CP le moins ambigu).
code_commune_reconstitue as (

    select id_pdc_itinerance, code_commune_insee as code_commune_reconstitue, methode
    from (
        select
            *,
            row_number() over (
                partition by id_pdc_itinerance
                order by nom_trouve_dans_adresse desc, taille_groupe_cp asc
            ) as rn,
            case
                when taille_groupe_cp = 1 then 'cp_unique'
                when nom_trouve_dans_adresse then 'cp_nom_trouve_dans_adresse'
                else 'cp_ambigu_non_resolu'
            end as methode
        from candidats_matches
    ) ranked
    where rn = 1 and (taille_groupe_cp = 1 or nom_trouve_dans_adresse)

),

-- code_insee_commune d'origine complété par la reconstitution quand elle a
-- réussi, avec une colonne de traçabilité (jamais de remplissage silencieux).
avec_code_commune_trace as (

    select
        b.id_pdc_itinerance,
        b.id_station_itinerance,
        b.id_pdc_local,
        b.adresse_station,
        b.puissance_nominale,
        b.date_maj,
        b.last_modified,
        b.consolidated_is_code_insee_verified,
        coalesce(nullif(b.code_insee_commune, ''), rec.code_commune_reconstitue) as code_insee_commune,
        case
            when b.code_insee_commune is not null and b.code_insee_commune <> '' then 'brut'
            when rec.code_commune_reconstitue is not null then rec.methode
            else 'non_resolu_probable_hors_france'
        end as code_commune_origine
    from irve_brut b
    left join code_commune_reconstitue rec on rec.id_pdc_itinerance = b.id_pdc_itinerance

),

-- Clé de dédoublonnage : id_pdc_itinerance, sauf placeholder "Non concerné"
-- (repli sur id_pdc_local), sinon clé unique par ligne (ROW_NUMBER) -- ne
-- fusionne jamais par défaut faute de preuve que c'est un doublon.
avec_cle_dedup as (

    select
        *,
        coalesce(
            nullif(
                case when lower(trim(id_pdc_itinerance)) = 'non concerné' then null
                     else nullif(trim(id_pdc_itinerance), '') end,
                ''
            ),
            nullif('LOCAL::' || id_pdc_local, 'LOCAL::'),
            'ROW::' || row_number() over ()
        ) as cle_dedup_pdc
    from avec_code_commune_trace

)

select
    id_pdc_itinerance,
    id_station_itinerance,
    id_pdc_local,
    adresse_station,
    puissance_nominale,
    date_maj,
    last_modified,
    consolidated_is_code_insee_verified,
    code_insee_commune,
    code_commune_origine
from (
    select
        *,
        row_number() over (
            partition by cle_dedup_pdc
            order by date_maj desc nulls last, last_modified desc nulls last
        ) as rn
    from avec_cle_dedup
) deduped
where rn = 1
