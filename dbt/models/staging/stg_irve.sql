-- Typage léger sur raw__irve__irve_consolide, équivalent de la vue DuckDB
-- irve_source (ve_pipeline/jointure/build_staging.py). Colonnes déjà
-- restreintes en amont par postgres_loader.KEPT_COLUMNS : code_insee_commune,
-- id_station_itinerance, puissance_nominale, consolidated_is_code_insee_verified,
-- adresse_station, id_pdc_itinerance, id_pdc_local, date_maj, last_modified.
select
    code_insee_commune,
    id_station_itinerance,
    id_pdc_itinerance,
    id_pdc_local,
    adresse_station,
    date_maj,
    last_modified,
    {{ safe_cast('puissance_nominale', 'double precision') }} as puissance_nominale,
    (lower(trim(consolidated_is_code_insee_verified)) = 'true') as consolidated_is_code_insee_verified
from {{ source('raw', 'raw__irve__irve_consolide') }}
