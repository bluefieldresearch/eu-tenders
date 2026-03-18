# FR_BOAMP — Bulletin Officiel des Annonces des Marches Publics

## Overview

France's official gazette for public procurement notices. Published by DILA (Direction de l'information legale et administrative). Covers tender announcements, award results, amendments, and cancellations from all French public authorities.

- **Website**: https://www.boamp.fr
- **Country**: France
- **Coverage**: All French public procurement above adapted procedure thresholds, plus concessions/DSP
- **Data format**: JSON via OpenDataSoft API
- **Update frequency**: Twice daily
- **Data range**: March 2015 to present
- **License**: Licence Ouverte / Open Licence 2.0

## Relationship to FR_DECP

FR_BOAMP and FR_DECP are complementary French sources:

| Aspect | FR_BOAMP | FR_DECP |
|--------|----------|---------|
| Content | Notices (announcements + awards) | Awarded contracts only |
| Concessions/DSP | Yes | No |
| Pre-award tenders | Yes | No |
| Awardee details | Name only | Name + SIRET |
| CPV codes | In nested JSON | Direct field |
| Contract amounts | Estimated value only | Award amount |
| Awardee SIRET | Not available | Available |

FR_BOAMP fills the gaps that FR_DECP cannot: **tender announcements** (before award) and **concessions/delegations de service public**.

## Data Access

Data is accessed via the OpenDataSoft API:

```
https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records
```

**API characteristics:**
- No authentication required
- Max 100 records per request (`limit=100`)
- Max 10,000 offset per query — pagination must use date partitioning (monthly)
- Supports ODSQL filtering via `where` parameter

**Documentation:**
- API explorer: https://boamp-datadila.opendatasoft.com/explore/dataset/boamp/api/
- Official open data page: https://www.boamp.fr/pages/donnees-ouvertes-et-api/

## Sync Script

```bash
python3 source/fr_boamp.py --from 2026-01-01 --to 2026-03-18
python3 source/fr_boamp.py --from 2026-03-01  # defaults --to to today
```

The script iterates month by month (to stay under the 10K offset limit), fetches all records from the API, and upserts into the `contracts` table.

**Performance**: ~120 records/second (limited by API response size and 100-record page limit).

## Notice Lifecycle and Deduplication

BOAMP publishes separate notices for each stage of a tender's lifecycle. To avoid duplicate rows, the script handles this as follows:

1. **`APPEL_OFFRE`** (tender announcement) — creates a new row with `source_id = idweb`, `status = 'announced'`, `awardee = NULL`
2. **`ATTRIBUTION`** (award notice) — **updates the existing row** using the `annonce_lie` field to find the original announcement's `idweb`. Sets `status = 'awarded'`, populates `awardee` and `date_awarded`.
3. **`ANNULATION`** (cancellation) — creates/updates with `status = 'cancelled'`
4. **`PRE-INFORMATION`** (prior information) — creates with `status = 'prior-notice'`

The following notice types are **excluded** to avoid noise:
- `RECTIFICATIF` — amendments to existing notices
- `MODIFICATION` — contract modifications
- `INTENTION_CONCLURE`, `EX_ANTE_VOLONTAIRE`, `PERIODIQUE`, `QUALIFICATION`, `AUTRE`

**Import order**: Records are fetched in `dateparution ASC` order so announcements are processed before their corresponding awards.

## Record Distribution

| Notice type | Description | Records |
|------------|-------------|---------|
| `APPEL_OFFRE` | Tender announcements | ~1.12M |
| `ATTRIBUTION` | Award results | ~450K |
| `ANNULATION` | Cancellations | ~400 |
| `PRE-INFORMATION` | Prior information notices | ~2.7K |

### By famille (category)

| Famille | Description |
|---------|-------------|
| `JOUE` | EU-level notices (Journal Officiel de l'Union Europeenne) |
| `FNS` | National-level formal notices |
| `MAPA` | Adapted procedure notices (below EU thresholds) |
| `DSP` | Concessions / Delegations de service public |

## API Fields

| Field | Type | Description |
|-------|------|-------------|
| `idweb` | string | Unique notice identifier (e.g., `26-25679`) |
| `dateparution` | date | Publication date |
| `datefindiffusion` | date | Notice validity end date |
| `datelimitereponse` | datetime | Response deadline |
| `nomacheteur` | string | Contracting authority name |
| `titulaire` | array/string | Awardee name(s) — null if not yet awarded |
| `objet` | string | Contract subject/title |
| `nature` | enum | Notice type: `APPEL_OFFRE`, `ATTRIBUTION`, `RECTIFICATIF`, etc. |
| `famille` | enum | Category: `JOUE`, `FNS`, `MAPA`, `DSP` |
| `type_marche` | array | Contract type(s): `TRAVAUX`, `SERVICES`, `FOURNITURES` |
| `perimetre` | string | Scope/directive |
| `procedure_libelle` | string | Procedure name (French) |
| `code_departement` | array | Department code(s) |
| `annonce_lie` | array | Linked notice idweb(s) — used to connect awards to announcements |
| `etat` | enum | State: `INITIAL`, `RECTIFICATIF` |
| `url_avis` | string | Link to notice on boamp.fr |
| `gestion` | JSON string | Metadata (publication dates, descriptors) |
| `donnees` | JSON string | Detailed specifications (CPV, NUTS, lots, values, buyer details) |

### Nested `donnees` structure

The `donnees` field is a JSON-encoded string containing rich procurement details. Key paths:

| JSON path | Description |
|-----------|-------------|
| `*.initial.descriptionMarche.titreMarche` | Contract title |
| `*.initial.descriptionMarche.numeroReference` | Reference number |
| `*.initial.descriptionMarche.CPV.objetPrincipal.classPrincipale` | Primary CPV code |
| `*.initial.descriptionMarche.lot.estimationValeur.valeur` | Estimated value |
| `*.initial.descriptionMarche.lot.lieuCodeNUTS.codeNUTS` | NUTS code |
| `*.initial.descriptionMarche.lot.lieuExecutionLivraison` | Place of execution |
| `*.initial.descriptionMarche.lot.dureeLot.dateACompterDu` | Contract start date |
| `*.initial.descriptionMarche.lot.dureeLot.dateJusquau` | Contract end date |
| `*.organisme.acheteurPublic` | Buyer name |
| `*.organisme.codeNUTS` | Buyer NUTS code |

The root key varies: `DSP` for concessions, `MARCHE` for regular contracts.

## Field Mapping

| Source field | contracts column | Transformation |
|-------------|-----------------|----------------|
| `idweb` | `source_id` | For `ATTRIBUTION`: uses `annonce_lie[0]` instead (links to original announcement) |
| `donnees.*.numeroReference` | `reference_number` | Parsed from nested JSON |
| `url_avis` | `source_url` | Used as-is |
| `nature` | `status` | See status mapping below |
| `type_marche[0]` | `contract_nature` | See type mapping below |
| `famille` | `is_concession` | `true` if `DSP` |
| `type_marche[0]` | `contract_type_original` | Used as-is |
| `procedure_libelle` | `procedure_type` | See procedure mapping below |
| `nature` | `notice_type` | See notice mapping below |
| `objet` | `contract_title` | Used as-is |
| `donnees.*.lot.dureeLot` | `contract_duration` | `"{start} to {end}"` |
| `nomacheteur` | `contracting_authority` | Used as-is |
| `donnees.*.lot.lieuExecutionLivraison` or `code_departement` | `place_of_execution` | Fallback chain |
| `donnees.*.lot.lieuCodeNUTS.codeNUTS` or `donnees.*.organisme.codeNUTS` | `nuts_code` | Parsed from nested JSON |
| `donnees.*.lot.estimationValeur.valeur` | `estimated_value` | Float, parsed from nested JSON |
| `titulaire` | `awardee` | Joined with ` \| ` if multiple |
| `dateparution` | `date_published`, `date_updated` | Used as-is |
| `dateparution` (on ATTRIBUTION) | `date_awarded` | Only set for award notices |
| `donnees.*.CPV.objetPrincipal.classPrincipale` | `cpv_codes` | Parsed from nested JSON, single-element array |
| — | `country` | Always `'FR'` |
| — | `*_currency` | Always `'EUR'` |

## Status Mapping

| `nature` | `status` |
|----------|---------|
| `APPEL_OFFRE` | `announced` |
| `ATTRIBUTION` | `awarded` |
| `ANNULATION` | `cancelled` |
| `PRE-INFORMATION` | `prior-notice` |

## Contract Type Mapping

| `type_marche` | `contract_nature` |
|--------------|------------------|
| `TRAVAUX` | `works` |
| `SERVICES` | `services` |
| `FOURNITURES` | `supplies` |

## Notice Type Mapping

| `nature` | `notice_type` |
|----------|--------------|
| `APPEL_OFFRE` | `cn-standard` |
| `ATTRIBUTION` | `can-standard` |
| `ANNULATION` | `cn-standard` |
| `PRE-INFORMATION` | `pin-only` |

## Procedure Type Mapping

| `procedure_libelle` (contains) | `procedure_type` |
|-------------------------------|-----------------|
| ouverte/ouvert | `open` |
| adaptee/adapte | `open` |
| restreint | `restricted` |
| negocie/negociation | `neg-w-call` |
| dialogue | `comp-dial` |

## Primary Key

`(source='FR_BOAMP', source_id, lot_number)`

- `source_id`: The `idweb` of the **original announcement**. For `ATTRIBUTION` notices, `annonce_lie[0]` is used instead of the attribution's own `idweb`, so the award updates the announcement row.
- `lot_number`: Always `'0'` (lot-level detail not extracted)

## Limitations

- **No awardee tax ID**: BOAMP does not provide SIRET/SIREN for awardees (use FR_DECP for that)
- **No award amounts**: The `titulaire` field has the name but not the awarded amount
- **10K offset limit**: The OpenDataSoft API returns 400 Bad Request for offsets >= 10,000, requiring month-by-month pagination
- **Slow API**: Max 100 records per request, ~120 rec/s throughput
- **Nested JSON parsing**: CPV codes, NUTS codes, and estimated values are buried in a JSON-encoded string field (`donnees`), with different structures for DSP vs regular contracts
