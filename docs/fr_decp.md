# FR_DECP — Donnees Essentielles de la Commande Publique

## Overview

France's consolidated essential public procurement data. Aggregates contract award data from multiple sources including PES Marche (DGFiP), DUME API (AIFE), and various regional software vendors. Published on data.gouv.fr.

- **Website**: https://data.gouv.fr
- **Country**: France
- **Coverage**: All French public buyers — state, regions, departments, communes, public bodies, hospitals, universities
- **Data format**: Parquet (189 MB) or CSV (1.9 GB)
- **Update frequency**: Daily
- **License**: Licence Ouverte / Open Licence 2.0

## Data Access

The consolidated tabular dataset is published at:

https://www.data.gouv.fr/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/

Available resources:

| File | Format | Size |
|------|--------|------|
| `decp.parquet` | Parquet | ~189 MB |
| `decp.csv` | CSV | ~1.9 GB |
| `schema.json` | JSON | ~18 KB |

The script downloads the Parquet file (significantly smaller than CSV) and caches it at `/tmp/decp.parquet`.

The dataset URL is resolved dynamically via the data.gouv.fr API:

```
GET https://www.data.gouv.fr/api/1/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/
```

## Sync Script

```bash
python3 source/fr_decp.py                                    # full load
python3 source/fr_decp.py --from 2024-01-01 --to 2024-12-31  # date range filter
python3 source/fr_decp.py --from 2026-01-01                   # from date to today
```

The script downloads the full Parquet file, filters to `donneesActuelles=True` (current data only), optionally filters by `dateNotification` range, and upserts into the `contracts` table.

**Important**: Unlike ES_PLACE and ES_GENCAT which support true incremental sync, FR_DECP always downloads the full Parquet file. The `--from`/`--to` filters are applied locally after download. The upsert ensures existing records are updated and new ones inserted.

## Data Characteristics

- **Only awarded contracts**: DECP contains notified/awarded contracts, not tender announcements. All records are mapped to `status='formalized'`.
- **`donneesActuelles` flag**: The dataset contains historical modifications. Only rows with `donneesActuelles=True` represent the current state of each contract.
- **`modification_id`**: Tracks contract modifications. `0` = original data, `1+` = subsequent modifications. The `uid` stays the same across modifications.
- **Single CPV code per record**: Unlike Spanish sources which can have multiple CPV codes, DECP provides one CPV code per contract.

## Source Fields

| Field | Type | Description |
|-------|------|-------------|
| `uid` | string | Unique ID (SIRET + contract ID concatenated) |
| `id` | string | Contract reference assigned by buyer |
| `nature` | string | Contract nature (Marche, Accord-cadre, etc.) |
| `type` | string | Contract type (Travaux, Services, Fournitures) |
| `objet` | string | Contract subject (may be truncated to 256 or 1000 chars) |
| `codeCPV` | string | CPV code (single code, no separator) |
| `procedure` | string | Procurement procedure |
| `montant` | number | Amount in EUR (excl. tax) — forfait or max estimated |
| `dureeMois` | integer | Duration in months |
| `offresRecues` | integer | Number of offers received |
| `dateNotification` | date | Contract notification date |
| `datePublicationDonnees` | date | Data publication date |
| `acheteur_id` | integer | Buyer SIRET |
| `acheteur_nom` | string | Buyer name (from INSEE SIRENE) |
| `acheteur_categorie` | string | Buyer category (Commune, Region, EPIC, etc.) |
| `acheteur_commune_nom` | string | Buyer commune |
| `acheteur_departement_nom` | string | Buyer department |
| `acheteur_region_nom` | string | Buyer region |
| `titulaire_id` | integer | Awardee SIRET |
| `titulaire_nom` | string | Awardee name (from INSEE SIRENE) |
| `titulaire_typeIdentifiant` | string | ID type (SIRET, SIREN, etc.) |
| `formePrix` | string | Price form (Unitaire, Forfaitaire, Mixte) |
| `marcheInnovant` | boolean | Innovative procurement flag |
| `considerationsSociales` | string | Social considerations |
| `considerationsEnvironnementales` | string | Environmental considerations |
| `sourceDataset` | string | Source dataset code |
| `sourceFile` | string | Link to source file |
| `donneesActuelles` | boolean | Whether this is the current version |
| `modification_id` | integer | Modification sequence (0 = original) |

## Field Mapping

| Source field | contracts column | Transformation |
|-------------|-----------------|----------------|
| `uid` | `source_id` | Used as-is |
| `id` | `reference_number` | Used as-is |
| `sourceFile` | `source_url` | Used as-is |
| — | `status` | Always `'formalized'` (DECP only has awarded contracts) |
| `type` | `contract_nature` | See type mapping below |
| `nature` | `is_concession` | `true` for concession/delegation natures |
| `type` | `contract_type_original` | Used as-is (French) |
| `procedure` | `procedure_type` | See procedure mapping below |
| — | `notice_type` | Always `'can-standard'` |
| `objet` | `contract_title` | Used as-is |
| `dureeMois` | `contract_duration` | `"{months} MON"` |
| `acheteur_nom` | `contracting_authority` | Used as-is |
| `acheteur_id` | `authority_id` | SIRET as string |
| `acheteur_categorie` | `authority_type` | See authority mapping below |
| `acheteur_commune_nom, departement_nom, region_nom` | `place_of_execution` | Comma-joined |
| `montant` | `estimated_value` | Float |
| `montant` | `award_value` | Float (same as estimated for DECP) |
| `titulaire_nom` | `awardee` | Used as-is |
| `titulaire_id` | `awardee_id` | SIRET as string |
| `offresRecues` | `num_offers` | Integer |
| `datePublicationDonnees` | `date_published`, `date_updated` | Date |
| `dateNotification` | `date_awarded` | Date |
| `codeCPV` | `cpv_codes` | Single-element array |
| — | `country` | Always `'FR'` |
| — | `*_currency` | Always `'EUR'` |

## Contract Type Mapping

| Source value (French) | `contract_nature` |
|----------------------|------------------|
| Travaux | `works` |
| Services | `services` |
| Fournitures | `supplies` |
| Non categorise | `NULL` |

## Procedure Type Mapping

| Source value (French) | `procedure_type` |
|----------------------|-----------------|
| Appel d'offres ouvert | `open` |
| Procedure adaptee | `open` |
| Appel d'offres restreint | `restricted` |
| Procedure avec negociation | `neg-w-call` |
| Procedure concurrentielle avec negociation | `neg-w-call` |
| Procedure negociee avec mise en concurrence prealable | `neg-w-call` |
| Marche passe/negocie sans publicite ni mise en concurrence prealable | `neg-wo-call` |
| Dialogue competitif | `comp-dial` |

## Authority Type Mapping

| `acheteur_categorie` | `authority_type` |
|---------------------|-----------------|
| Etat | `cga` |
| Region | `ra` |
| Commune | `la` |
| Departement / Departement outre-mer | `la` |
| Groupement de communes | `la` |
| Syndicat mixte | `la` |
| EPIC | `body-pl` |
| Etablissement hospitalier | `body-pl` |

## Primary Key

`(source='FR_DECP', source_id, lot_number)`

- `source_id`: The `uid` field (e.g., `21420187300012202525MP009002`)
- `lot_number`: Always `'0'` (DECP does not provide lot-level granularity)

## Deduplication

The `donneesActuelles=True` filter ensures only the current version of each contract is imported. The `uid` field is stable across modifications, so the upsert naturally handles updates.
