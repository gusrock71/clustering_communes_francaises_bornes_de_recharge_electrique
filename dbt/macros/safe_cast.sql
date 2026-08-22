{#
    Équivalent du TRY_CAST DuckDB (utilisé côté ve_pipeline/jointure et
    ve_pipeline/cleaning pour les valeurs Enedis masquées / puissance
    IRVE) : Postgres n'a pas de TRY_CAST natif, un CAST direct sur une
    valeur non numérique lève une erreur dure et interromprait tout le
    modèle. On vérifie donc le format par regex avant de caster ; toute
    valeur qui ne matche pas devient NULL au lieu de faire planter la
    requête -- même philosophie défensive que côté DuckDB (masquage
    Enedis pour secret statistique, ex: valeur "s" au lieu d'un nombre).
#}
{% macro safe_cast(column_name, sql_type) %}
    case when {{ column_name }} ~ '^\s*-?[0-9]+(\.[0-9]+)?\s*$'
         then trim({{ column_name }})::{{ sql_type }}
         else null end
{% endmacro %}
