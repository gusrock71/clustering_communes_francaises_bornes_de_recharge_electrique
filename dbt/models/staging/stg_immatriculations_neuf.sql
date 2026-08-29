-- Fenêtre glissante de 8 exercices (décision du 2026-08-29, voir la macro
-- dbt/macros/immat_years.sql) : les colonnes immat_20XX sélectionnées ci-
-- dessous sont calculées dynamiquement à partir de la date d'exécution,
-- plus jamais une liste écrite en dur. `postgres_loader.py` applique le
-- même calcul côté chargement Postgres pour ne garder que ces mêmes 8
-- années, quelle que soit la largeur réelle du fichier source (confirmé le
-- 2026-08-29 : le fichier SDES contient un historique complet 2010-2025,
-- bien plus large que les 8 ans utilisés ici).
--
-- Filtre groupe/categorie (décision explicite du 2026-08-21, cf. section
-- "Filtre véhicules concernés par la recharge IRVE" sur Notion) : on ne
-- garde que les véhicules qui rechargent réellement sur le réseau de bornes
-- publiques IRVE modélisé par ce projet.
--   - VP, VUL : recharge Type 2/CCS standard sur bornes publiques -- coeur
--     de cible du projet.
--   - CATL/MOTOCYCLETTE : la plupart des motos électriques ont un
--     connecteur Type 2 et se rechargent bien sur les mêmes bornes AC que
--     les voitures (6-22 kW) -- pèsent réellement sur le réseau, à garder.
--   - CATL/CYCLOMOTEUR exclu : batterie amovible, rechargée à domicile sur
--     son propre chargeur, pas via une borne IRVE.
--   - CATL/VOITURETTE exclu : recharge native sur prise domestique
--     (ex. Citroën Ami, 1,8 kW) -- usage IRVE possible via adaptateur mais
--     marginal et impact négligeable sur le dimensionnement capacitaire.
--   - AUTRES (remorque, tracteur agricole), PL (camion, tracteur routier),
--     TCP (bus, autocar) exclus : infrastructure de recharge dédiée
--     (mégawatt, dépôt) hors périmètre MVP.
select
    commune_code as code_commune,
    carburant,
    {% for annee in immat_years() -%}
    {{ safe_cast('immat_' ~ annee, 'integer') }} as immat_{{ annee }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source('raw', 'raw__immatriculations__immatriculations_neuf') }}
where groupe in ('VP', 'VUL')
   or (groupe = 'CATL' and categorie = 'MOTOCYCLETTE')
