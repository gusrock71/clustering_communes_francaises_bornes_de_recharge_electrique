# Tester l'ingestion via Airbyte Cloud → S3 réel

Cheminement pour reproduire la brique d'ingestion J1 (IRVE, immatriculations, Enedis) avec **Airbyte Cloud**, en écrivant vers un vrai bucket S3.

> Remplace la première tentative en self-hosted (abctl) : Airbyte Cloud sert l'UI en HTTPS géré par Airbyte, donc pas de souci de cookie "Secure" bloqué comme en local HTTP, et pas de Docker/Kubernetes à faire tourner sur ta machine.

## 0. Inscription

1. Va sur https://cloud.airbyte.com/signup et crée un compte.
2. Essai gratuit : 14 jours, 400 crédits. Vérifie à l'inscription si une carte bancaire est demandée (ça a pu varier) — dans tous les cas tu ne seras pas facturé pendant l'essai.
3. Un workspace est créé automatiquement à la première connexion.

## 1. Préparer le bucket S3 réel (si pas déjà fait)

```bash
aws s3 mb s3://ve-pipeline-landing-airbyte-test --region eu-west-3
```

Utilisateur IAM dédié avec une policy minimale (remplace le nom du bucket) :

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::ve-pipeline-landing-airbyte-test",
        "arn:aws:s3:::ve-pipeline-landing-airbyte-test/*"
      ]
    }
  ]
}
```

Génère une Access key / Secret key pour cet utilisateur. Comme Airbyte Cloud tourne sur l'infra d'Airbyte (pas sur ton réseau), c'est cette paire de clés IAM qui authentifie l'écriture dans ton bucket — pas de règle réseau/pare-feu particulière à ouvrir de ton côté.

## 2. Créer les 4 sources dans l'UI Airbyte Cloud

Même logique qu'en self-hosted : le connecteur **File (CSV, JSON, Excel, Feather, Parquet)** lit directement une URL HTTPS, donc pas de code à écrire. Un connecteur File = un seul fichier, donc **4 sources** pour tes 3 sources métier (immatriculations a 2 fichiers) :

| Source Airbyte | Storage Provider | URL | Contenu |
|---|---|---|---|
| `irve_consolide` | HTTPS: Public Web | `https://www.data.gouv.fr/api/1/datasets/r/eb76d20a-8501-400e-b336-d85724de5435` | Bornes de recharge (IRVE) |
| `immatriculations_neuf` | HTTPS: Public Web | `https://www.data.gouv.fr/api/1/datasets/r/b2bac57a-7a25-4df1-9808-46b96b25311d` | Véhicules achetés **neufs**, par commune |
| `immatriculations_occasion` | HTTPS: Public Web | `https://www.data.gouv.fr/api/1/datasets/r/eac49e71-bad2-49a0-980f-59bc960f5e2c` | Véhicules achetés **d'occasion**, par commune |
| `enedis_conso_commune` | HTTPS: Public Web | `https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-electrique-par-secteur-dactivite-commune/exports/csv` | Consommation électrique par commune |

Pour chacune : **Sources → + New source → File (CSV, JSON, Excel, Feather, Parquet)** → format `csv` → colle l'URL → nom de dataset explicite (ceux du tableau) → **Set up source**, qui déclenche automatiquement un test de connexion. C'est ce test qui valide que la source est joignable et lisible — l'équivalent du test de connectivité côté pipeline Python.

## 3. Créer la destination S3

**Destinations → + New destination → S3**. Renseigne :
- Bucket name : `ve-pipeline-landing-airbyte-test`
- Région : `eu-west-3`
- Access key / Secret key : ceux créés à l'étape 1
- **S3 Bucket Path** : pour te rapprocher de la convention `raw/<source>/dt=<YYYY-MM-DD>/...` déjà utilisée côté Python, utilise les variables proposées dans l'écran de config (nom du stream, date de sync) — vérifie la syntaxe exacte des placeholders affichée dans l'UI, elle a pu évoluer entre versions.
- Format de sortie : CSV (pour rester au plus proche du brut, sans conversion Parquet/Avro).

**Test the destination connection** avant de continuer.

## 4. Créer les connexions et lancer un sync manuel

Pour chacune des 4 sources : **Connections → + New connection** → source correspondante → destination S3 → **Sync mode** : `Full refresh | Overwrite` (équivalent du "écrase à chaque run" côté Python) → fréquence **Manual** pour ce test.

Lance **Sync now** sur chaque connexion, suis le log de sync dans l'UI, puis vérifie le dépôt réel :

```bash
aws s3 ls s3://ve-pipeline-landing-airbyte-test/ --recursive
```

## 5. Comparer avec le pipeline Python

Points à évaluer pendant le test :
- Le connecteur File remonte-t-il une erreur claire si le fichier immatriculations change de schéma ? (pas de garde-fou custom type `schema_hint` possible sans passer par le Connector Builder)
- Lisibilité et niveau de détail du log de sync en cas d'échec réseau, comparé aux messages d'erreur du connecteur Python (`SourceUnreachableError`, `SchemaDriftError`).
- Temps de mise en place réel (signup + 4 sources + 1 destination + 4 connexions) vs le connecteur Python déjà opérationnel et testé.
- Consommation de crédits sur les 4 syncs, pour te faire une idée du coût si ça devait tourner en continu au-delà de l'essai.

## Repères

- Signup Airbyte Cloud : https://cloud.airbyte.com/signup
- Doc File source : https://docs.airbyte.com/integrations/sources/file
- Doc destination S3 : https://airbyte.com/connectors/s3-data-lake
