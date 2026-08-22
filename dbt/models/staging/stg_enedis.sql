-- Colonnes déjà restreintes en amont par postgres_loader.KEPT_COLUMNS
-- (allowlist, 8/49 colonnes -- code_commune, code_grand_secteur, nb_sites,
-- conso_totale_mwh, conso_moyenne_mwh, nombre_d_habitants,
-- part_thermosensible, taux_de_chauffage_electrique). safe_cast plutôt
-- qu'un CAST direct : certaines valeurs sont masquées (secret statistique,
-- ex: "s" au lieu d'un nombre) -- équivalent du TRY_CAST DuckDB.
select
    code_commune,
    code_grand_secteur,
    {{ safe_cast('nb_sites', 'double precision') }} as nb_sites,
    {{ safe_cast('conso_totale_mwh', 'double precision') }} as conso_totale_mwh,
    {{ safe_cast('conso_moyenne_mwh', 'double precision') }} as conso_moyenne_mwh,
    {{ safe_cast('nombre_d_habitants', 'double precision') }} as nombre_d_habitants,
    {{ safe_cast('part_thermosensible', 'double precision') }} as part_thermosensible,
    {{ safe_cast('taux_de_chauffage_electrique', 'double precision') }} as taux_de_chauffage_electrique
from {{ source('raw', 'raw__enedis_conso__enedis_conso_commune_2024') }}
