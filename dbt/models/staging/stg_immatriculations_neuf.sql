-- Colonnes déjà restreintes en amont par postgres_loader.DROPPED_COLUMNS :
-- IMMAT_2010...IMMAT_2017 exclus (contrainte de quota Neon, décision
-- explicite du 2026-08-21 -- voir le docstring de postgres_loader.py). Ce
-- modèle ne peut donc PAS reproduire le détail immat_ve_2010...immat_ve_2017
-- que le pipeline DuckDB/S3 (gardé en sauvegarde) calculait.
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
    {{ safe_cast('immat_2018', 'integer') }} as immat_2018,
    {{ safe_cast('immat_2019', 'integer') }} as immat_2019,
    {{ safe_cast('immat_2020', 'integer') }} as immat_2020,
    {{ safe_cast('immat_2021', 'integer') }} as immat_2021,
    {{ safe_cast('immat_2022', 'integer') }} as immat_2022,
    {{ safe_cast('immat_2023', 'integer') }} as immat_2023,
    {{ safe_cast('immat_2024', 'integer') }} as immat_2024,
    {{ safe_cast('immat_2025', 'integer') }} as immat_2025
from {{ source('raw', 'raw__immatriculations__immatriculations_neuf') }}
where groupe in ('VP', 'VUL')
   or (groupe = 'CATL' and categorie = 'MOTOCYCLETTE')
