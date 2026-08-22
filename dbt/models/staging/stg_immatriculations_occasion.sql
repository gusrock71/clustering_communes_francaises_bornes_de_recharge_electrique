-- Voir stg_immatriculations_neuf.sql pour la note sur IMMAT_2010...2017
-- exclus, et pour la justification détaillée du filtre groupe/categorie
-- ci-dessous (décision explicite du 2026-08-21). crit_air non repris ici :
-- pas utilisé par int_territorial_join (ni par l'équivalent DuckDB
-- build_staging.py).
select
    commune_code as code_commune,
    carburant,
    {{ safe_cast('immat_2018', 'integer') }} as immat_2018,
    {{ safe_cast('immat_2019', 'integer') }} as immat_2019,
    {{ safe_cast('immat_2020', 'integer') }} as immat_2020,
    {{ safe_cast('immat_2021', 'integer') }} as immat_2021,
    {{ safe_cast('immat_2022', 'integer') }} as immat_2022,
    {{ safe_cast('immat_2023', 'integer') }} as immat_2023,
    {{ safe_cast('immat_2024', 'integer') }} as immat_2024,
    {{ safe_cast('immat_2025', 'integer') }} as immat_2025
from {{ source('raw', 'raw__immatriculations__immatriculations_occasion') }}
where groupe in ('VP', 'VUL')
   or (groupe = 'CATL' and categorie = 'MOTOCYCLETTE')
