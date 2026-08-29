-- Port dbt/Postgres de ve_pipeline/features/build_clustering_dataset.py
-- (gardé en sauvegarde DuckDB/S3). Décisions produit du 2026-08-19,
-- inchangées :
--   1. Exclusion des communes has_immatriculations=false ou has_enedis=false.
--   2. croissance_immat_ve_pct : imputation par la médiane des croissances
--      réellement calculables pour demarrage_ve_tardif=true (Option A,
--      valeur neutre) ; sinon COALESCE à 0 (vraie absence de VE observée).
--   3. Colonnes Enedis : imputation du résidu masqué (secret statistique)
--      par la médiane, calculée dynamiquement -- jamais de valeur en dur.
--
-- Postgres n'a pas de `SELECT * EXCLUDE(...)` (extension DuckDB) : les
-- colonnes de int_territorial_join sont donc listées explicitement plutôt
-- que passées telles quelles.
--
-- Features de "couverture" ajoutées le 2026-08-21 (croisement souhaité par
-- l'utilisateur : communes sous/bien équipées en VE x communes en
-- risque/bien desservies électriquement) :
--   - nb_ve_stock_estime : proxy du parc VE en circulation par commune,
--     somme des immatriculations sur la fenêtre glissante de 8 exercices
--     (immat_ve_annee_1...immat_ve_annee_8, macro immat_years() -- noms
--     positionnels, pas calendaires, depuis le 2026-08-29, voir
--     int_territorial_join.sql). C'est un flux cumulé, pas un vrai stock
--     (ignore la casse/les départs de véhicules hors commune) --
--     approximation acceptée faute de mieux.
--   - taux_couverture_afir = puissance installée / (1,3 kW * stock VE),
--     1,3 kW = seuil réglementaire AFIR (UE, en vigueur depuis avril 2024)
--     par VE à batterie en circulation. 1 = conforme, <1 = sous-doté.
--   - ratio_pdc_par_100_ve = (nb PDC / stock VE) / benchmark national de la
--     zone (0,24 urbain / 0,10 rural -- moyenne Insee 2026, pas une norme
--     réglementaire). 1 = dans la moyenne nationale de sa zone.
--   - zone_densite sert UNIQUEMENT à ce calcul (benchmark + médiane par
--     zone) -- décision explicite de ne pas la garder comme feature de
--     sortie du clustering (déjà "absorbée" dans le ratio normalisé, elle
--     ferait doublon si répétée telle quelle).
--   - Stock VE = 0 (jamais un seul VE immatriculé 2018-2025, commune
--     retenue via has_immatriculations=true sur du thermique) : ratio
--     indéfini -> imputé par la médiane de la même zone (valeur neutre,
--     même principe Option A que croissance_immat_ve_pct), repli sur la
--     médiane globale si la zone n'a aucune valeur calculable.
--   - Stock VE > 0 mais très faible (1-3 VE) : ratio non indéfini mais
--     statistiquement instable (une seule PDC fait exploser le ratio) ->
--     plafonné à 3x le repère (var plafond_ratio_couverture) plutôt
--     qu'imputé, pour ne pas écraser les distances du K-Means avec
--     quelques communes minuscules.

with base as (

    select * from {{ ref('int_territorial_join') }}

),

with_stock as (

    select
        *,
        {% for i in range(1, 9) -%}
        coalesce(immat_ve_annee_{{ i }}, 0){% if not loop.last %} + {% endif %}
        {% endfor %} as nb_ve_stock_estime
    from base

),

ratios_brut as (

    select
        *,
        case when nb_ve_stock_estime > 0
             then puissance_totale_installee_kw / nb_ve_stock_estime::numeric / {{ var('afir_kw_par_ve') }}
             else null end as taux_couverture_afir_brut,
        case when nb_ve_stock_estime > 0
             then (nb_points_charge::numeric / nb_ve_stock_estime::numeric)
                  / (case when zone_densite = 'urbain' then {{ var('benchmark_pdc_urbain') }}
                          else {{ var('benchmark_pdc_rural') }} end)
             else null end as ratio_pdc_par_100_ve_brut
    from with_stock

),

-- Médiane calculée sur les valeurs déjà plafonnées (least(..., plafond)),
-- pas sur les brutes : sinon un outlier à faible stock (ex. 38 -- cf.
-- validation Neon du 2026-08-21) tire la médiane elle-même au-dessus du
-- plafond, et une commune imputée se retrouverait avec une valeur plus
-- extrême qu'une commune plafonnée -- incohérent avec l'objectif du
-- plafond (borner toutes les valeurs de la feature à [0, plafond]).
median_couverture_zone as (

    select
        zone_densite,
        percentile_cont(0.5) within group (
            order by least(taux_couverture_afir_brut, {{ var('plafond_ratio_couverture') }})
        ) as median_afir_zone,
        percentile_cont(0.5) within group (
            order by least(ratio_pdc_par_100_ve_brut, {{ var('plafond_ratio_couverture') }})
        ) as median_pdc_zone
    from ratios_brut
    where has_immatriculations = true and has_enedis = true and nb_ve_stock_estime > 0
    group by zone_densite

),

median_couverture_global as (

    select
        percentile_cont(0.5) within group (
            order by least(taux_couverture_afir_brut, {{ var('plafond_ratio_couverture') }})
        ) as median_afir_global,
        percentile_cont(0.5) within group (
            order by least(ratio_pdc_par_100_ve_brut, {{ var('plafond_ratio_couverture') }})
        ) as median_pdc_global
    from ratios_brut
    where has_immatriculations = true and has_enedis = true and nb_ve_stock_estime > 0

),

median_croissance as (

    select percentile_cont(0.5) within group (order by croissance_immat_ve_pct) as median_croissance
    from base
    where has_immatriculations = true and croissance_immat_ve_pct is not null

),

median_enedis as (

    select
        percentile_cont(0.5) within group (order by nb_sites_residentiel) as median_nb_sites_residentiel,
        percentile_cont(0.5) within group (order by conso_totale_residentielle_mwh) as median_conso_totale_residentielle_mwh,
        percentile_cont(0.5) within group (order by conso_moyenne_residentielle_mwh) as median_conso_moyenne_residentielle_mwh,
        percentile_cont(0.5) within group (order by population) as median_population,
        percentile_cont(0.5) within group (order by taux_chauffage_electrique) as median_taux_chauffage_electrique,
        percentile_cont(0.5) within group (order by part_thermosensible) as median_part_thermosensible
    from base
    where has_enedis = true

)

select
    b.code_commune,
    b.nb_stations,
    b.nb_points_charge,
    b.puissance_totale_installee_kw,
    b.puissance_moyenne_pdc_kw,
    b.part_recharge_rapide,
    b.pct_pdc_code_insee_verifie,
    {% for i in range(1, 9) -%}
    b.immat_ve_annee_{{ i }},
    {% endfor -%}
    b.immat_toutes_motorisations_recente,
    b.immat_ve_baseline,
    b.immat_ve_recent,
    -- demarrage_ve_tardif=true -> médiane (valeur neutre, Option A).
    -- Sinon -> COALESCE à 0 : une croissance NULL restante signifie "jamais
    -- eu de VE sur toute la période" (baseline ET récent tous deux à 0),
    -- une vraie valeur de 0, pas un remplissage arbitraire.
    case when b.demarrage_ve_tardif = true then mc.median_croissance
         else coalesce(b.croissance_immat_ve_pct, 0) end as croissance_immat_ve_pct,
    -- Conservé comme feature à part entière (pas redondant avec
    -- croissance_immat_ve_pct) : pour les communes démarrage_ve_tardif=true,
    -- la croissance ci-dessus est déjà une médiane imputée (valeur neutre),
    -- donc numériquement indiscernable d'une commune à croissance réelle
    -- moyenne sans ce flag.
    b.demarrage_ve_tardif,
    b.nb_ve_stock_estime,
    case when b.nb_ve_stock_estime = 0
         then coalesce(mcz.median_afir_zone, mcg.median_afir_global)
         else least(b.taux_couverture_afir_brut, {{ var('plafond_ratio_couverture') }}) end as taux_couverture_afir,
    case when b.nb_ve_stock_estime = 0
         then coalesce(mcz.median_pdc_zone, mcg.median_pdc_global)
         else least(b.ratio_pdc_par_100_ve_brut, {{ var('plafond_ratio_couverture') }}) end as ratio_pdc_par_100_ve,
    coalesce(b.nb_sites_residentiel, me.median_nb_sites_residentiel) as nb_sites_residentiel,
    coalesce(b.conso_totale_residentielle_mwh, me.median_conso_totale_residentielle_mwh) as conso_totale_residentielle_mwh,
    coalesce(b.conso_moyenne_residentielle_mwh, me.median_conso_moyenne_residentielle_mwh) as conso_moyenne_residentielle_mwh,
    coalesce(b.population, me.median_population) as population,
    coalesce(b.part_thermosensible, me.median_part_thermosensible) as part_thermosensible,
    coalesce(b.taux_chauffage_electrique, me.median_taux_chauffage_electrique) as taux_chauffage_electrique,
    b.has_irve,
    b.has_immatriculations,
    b.has_enedis
from ratios_brut b
cross join median_croissance mc
cross join median_enedis me
cross join median_couverture_global mcg
left join median_couverture_zone mcz on mcz.zone_densite = b.zone_densite
where b.has_immatriculations = true and b.has_enedis = true
