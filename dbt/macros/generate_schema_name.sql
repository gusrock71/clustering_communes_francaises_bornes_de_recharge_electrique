{#
    Par défaut, dbt concatène le schéma cible et le schéma custom
    (ex: "public_staging"). Pour ce projet (une seule base Neon, un seul
    utilisateur), on préfère des schémas lisibles tels quels : staging,
    intermediate, marts -- sans préfixe.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
