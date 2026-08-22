-- Port dbt/Postgres de la jointure territoriale DuckDB
-- (ve_pipeline/jointure/build_staging.py, gardé en sauvegarde). Même
-- logique métier, adaptée à ce qui est réellement disponible dans les
-- tables raw__* Postgres :
--   - IRVE : lit int_irve_cleaned (dédup + code_insee_commune reconstitué,
--     port de ve_pipeline/cleaning/irve_code_commune.py, ajouté le
--     2026-08-21) plutôt que stg_irve brut.
--   - Immatriculations : IMMAT_2018...IMMAT_2025 uniquement (2010-2017
--     exclus dès le chargement Postgres, décision explicite du 2026-08-21).
--     Le détail immat_ve_2010...immat_ve_2017 du pipeline DuckDB original
--     n'est donc PAS reproduit ici -- écart accepté par l'utilisateur.
--   - Enedis : restreint au secteur RESIDENTIEL, casts défensifs (safe_cast)
--     pour les valeurs masquées (secret statistique).
--   - zone_densite (ajouté le 2026-08-21) : grille de densité Insee (seed
--     ref_densite_communes), colonne DENS (3 postes) uniquement, regroupée
--     en 'urbain' (Urbain dense + Urbain intermédiaire) / 'rural' (Rural).
--     Variable de contexte territorial, pas utilisée dans les agrégations
--     ci-dessous -- simple LEFT JOIN final sur code_commune.

with irve_commune as (

    select
        code_insee_commune as code_commune,
        count(distinct id_station_itinerance) as nb_stations,
        count(*) as nb_points_charge,
        sum(puissance_nominale) as puissance_totale_installee_kw,
        avg(puissance_nominale) as puissance_moyenne_pdc_kw,
        avg(case when puissance_nominale > 50 then 1.0 else 0.0 end) as part_recharge_rapide,
        avg(case when consolidated_is_code_insee_verified then 1.0 else 0.0 end) as pct_pdc_code_insee_verifie
    from {{ ref('int_irve_cleaned') }}
    where code_insee_commune is not null and code_insee_commune <> ''
    group by code_insee_commune

),

immat_union as (

    select code_commune, carburant, immat_2018, immat_2019, immat_2020, immat_2021, immat_2022, immat_2023, immat_2024, immat_2025
    from {{ ref('stg_immatriculations_neuf') }}

    union all

    select code_commune, carburant, immat_2018, immat_2019, immat_2020, immat_2021, immat_2022, immat_2023, immat_2024, immat_2025
    from {{ ref('stg_immatriculations_occasion') }}

),

-- COALESCE(..., 0) sur chaque SUM(...) FILTER(...) : en SQL, FILTER renvoie
-- NULL (pas 0) quand aucune ligne de la commune ne correspond au filtre
-- carburant -- même bug/fix que côté DuckDB (build_staging.py, corrigé le
-- 2026-08-19), FILTER se comporte pareil sous Postgres.
immat_commune as (

    select
        code_commune,
        coalesce(sum(immat_2018) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2018,
        coalesce(sum(immat_2019) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2019,
        coalesce(sum(immat_2020) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2020,
        coalesce(sum(immat_2021) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2021,
        coalesce(sum(immat_2022) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2022,
        coalesce(sum(immat_2023) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2023,
        coalesce(sum(immat_2024) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2024,
        coalesce(sum(immat_2025) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')), 0) as immat_ve_2025,
        sum(immat_2025) as immat_toutes_motorisations_2025,
        coalesce(
            sum(immat_2018 + immat_2019 + immat_2020) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')),
            0
        ) as immat_ve_baseline_2018_2020,
        coalesce(
            sum(immat_2023 + immat_2024 + immat_2025) filter (where carburant in ('Electrique et hydrogène', 'Hybride rechargeable')),
            0
        ) as immat_ve_recent_2023_2025
    from immat_union
    where code_commune is not null and code_commune <> ''
    group by code_commune

),

enedis_commune as (

    select
        code_commune,
        sum(nb_sites) as nb_sites_residentiel,
        sum(conso_totale_mwh) as conso_totale_residentielle_mwh,
        avg(conso_moyenne_mwh) as conso_moyenne_residentielle_mwh,
        max(nombre_d_habitants) as population,
        avg(part_thermosensible) as part_thermosensible,
        avg(taux_de_chauffage_electrique) as taux_chauffage_electrique
    from {{ ref('stg_enedis') }}
    where code_commune is not null and code_commune <> '' and code_grand_secteur = 'RESIDENTIEL'
    group by code_commune

),

densite as (

    select code_commune, zone_densite
    from {{ ref('ref_densite_communes') }}

),

-- Toutes les communes vues dans AU MOINS une source (pas d'INNER JOIN,
-- sinon on perdrait silencieusement les communes absentes d'une source).
communes_spine as (

    select distinct code_commune from irve_commune
    union
    select distinct code_commune from immat_commune
    union
    select distinct code_commune from enedis_commune

)

select
    s.code_commune,
    d.zone_densite,
    coalesce(i.nb_stations, 0) as nb_stations,
    coalesce(i.nb_points_charge, 0) as nb_points_charge,
    coalesce(i.puissance_totale_installee_kw, 0) as puissance_totale_installee_kw,
    coalesce(i.puissance_moyenne_pdc_kw, 0) as puissance_moyenne_pdc_kw,
    coalesce(i.part_recharge_rapide, 0) as part_recharge_rapide,
    i.pct_pdc_code_insee_verifie,
    m.immat_ve_2018,
    m.immat_ve_2019,
    m.immat_ve_2020,
    m.immat_ve_2021,
    m.immat_ve_2022,
    m.immat_ve_2023,
    m.immat_ve_2024,
    m.immat_ve_2025,
    m.immat_toutes_motorisations_2025,
    m.immat_ve_baseline_2018_2020,
    m.immat_ve_recent_2023_2025,
    case when m.immat_ve_baseline_2018_2020 > 0
         then (m.immat_ve_recent_2023_2025 - m.immat_ve_baseline_2018_2020) * 1.0 / m.immat_ve_baseline_2018_2020
         else null end as croissance_immat_ve_pct,
    case when m.code_commune is null then null
         when m.immat_ve_baseline_2018_2020 = 0 and m.immat_ve_recent_2023_2025 > 0 then true
         else false end as demarrage_ve_tardif,
    e.nb_sites_residentiel,
    e.conso_totale_residentielle_mwh,
    e.conso_moyenne_residentielle_mwh,
    e.population,
    e.part_thermosensible,
    e.taux_chauffage_electrique,
    (i.code_commune is not null) as has_irve,
    (m.code_commune is not null) as has_immatriculations,
    (e.code_commune is not null) as has_enedis
from communes_spine s
left join irve_commune  i on i.code_commune = s.code_commune
left join immat_commune m on m.code_commune = s.code_commune
left join enedis_commune e on e.code_commune = s.code_commune
left join densite        d on d.code_commune = s.code_commune
