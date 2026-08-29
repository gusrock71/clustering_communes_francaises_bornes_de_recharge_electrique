-- Voir stg_immatriculations_neuf.sql pour la note sur la fenêtre glissante
-- de 8 exercices (macro immat_years(), décision du 2026-08-29), et pour la
-- justification détaillée du filtre groupe/categorie ci-dessous (décision
-- explicite du 2026-08-21). crit_air non repris ici : pas utilisé par
-- int_territorial_join (ni par l'équivalent DuckDB build_staging.py).
select
    commune_code as code_commune,
    carburant,
    {% for annee in immat_years() -%}
    {{ safe_cast('immat_' ~ annee, 'integer') }} as immat_{{ annee }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source('raw', 'raw__immatriculations__immatriculations_occasion') }}
where groupe in ('VP', 'VUL')
   or (groupe = 'CATL' and categorie = 'MOTOCYCLETTE')
