# ES_GENCAT — Plataforma de Contractacio Publica de Catalunya

## Overview

Catalunya's regional public procurement platform. Publishes tenders and results from the Generalitat de Catalunya, local authorities, universities, and other public entities in Catalunya.

- **Website**: https://contractaciopublica.cat
- **Country**: Spain (Catalunya)
- **Coverage**: All Catalan public bodies — Generalitat departments, local authorities, universities, independent bodies
- **Data format**: JSON (Socrata API)
- **Update frequency**: Continuous (records updated as contract lifecycle progresses)

## Data Access

Data is accessed via the Socrata Open Data API hosted on Catalunya's Transparency portal:

```
https://analisi.transparenciacatalunya.cat/resource/ybgg-dgi6.json
```

The API supports:
- **Pagination**: `$limit` (max 50,000) and `$offset`
- **Filtering**: `$where` clause with SoQL syntax
- **Date filtering**: `:updated_at` system field for incremental sync
- **Ordering**: `$order` for consistent pagination

## Sync Script

```bash
python3 source/es_gencat.py --from 2025-02-01 --to 2025-02-28
python3 source/es_gencat.py --from 2025-03-01  # defaults --to to today
python3 source/es_gencat.py --from 2025-01-01 --target bigquery
```

The script queries the Socrata API with `:updated_at` date filtering, transforms each record, and upserts into the `contracts` table. Supports both `--target postgres` (default) and `--target bigquery`.

## API Fields

| API field | Type | Description |
|-----------|------|-------------|
| `codi_expedient` | string | Reference number |
| `codi_organ` | string | Contracting body code |
| `nom_organ` | string | Contracting body name |
| `codi_departament_ens` | string | Department code |
| `nom_departament_ens` | string | Department name |
| `codi_ambit` | string | Scope code |
| `nom_ambit` | string | Scope name (authority category) |
| `codi_dir3` | string | DIR3 code |
| `codi_ine10` | string | INE code |
| `tipus_contracte` | string | Contract type (Catalan) |
| `procediment` | string | Procurement procedure (Catalan) |
| `fase_publicacio` | string | Publication phase |
| `denominacio` | string | Tender denomination/title |
| `objecte_contracte` | string | Contract subject |
| `codi_cpv` | string | CPV codes (`\|\|` separated) |
| `valor_estimat_contracte` | number | Estimated value |
| `pressupost_base_licitacio` | number | Base budget |
| `durada_contracte` | string | Contract duration (free text) |
| `data_publicacio_anunci` | timestamp | Notice publication date |
| `data_publicacio_licitacio` | timestamp | Tender publication date |
| `data_publicacio_adjudicacio` | timestamp | Award publication date |
| `data_publicacio_formalitzacio` | timestamp | Formalization publication date |
| `data_publicacio_contracte` | timestamp | Contract publication date |
| `data_adjudicacio_contracte` | timestamp | Award date |
| `data_formalitzacio_contracte` | timestamp | Formalization date |
| `numero_lot` | string | Lot number |
| `identificacio_adjudicatari` | string | Awardee tax ID |
| `denominacio_adjudicatari` | string | Awardee name |
| `import_adjudicacio_sense` | number | Award amount (excl. tax) |
| `import_adjudicacio_amb_iva` | number | Award amount (incl. tax) |
| `ofertes_rebudes` | integer | Number of offers received |
| `resultat` | string | Result status |
| `es_agregada` | string | Aggregated publication flag |
| `enllac_publicacio` | object/string | Publication link (URL) |
| `tipus_tramitacio` | string | Processing type |
| `tipus_identificacio` | string | ID type |
| `codi_nuts` | string | NUTS code |
| `lloc_execucio` | string | Place of execution |
| `financament_europeu` | string | EU funding flag |

## Field Mapping

| Source field | contracts column | Transformation |
|-------------|-----------------|----------------|
| `codi_expedient + '/' + codi_organ` | `source_id` | Concatenated as composite key |
| `codi_expedient` | `reference_number` | Used as-is |
| `enllac_publicacio.url` | `source_url` | Extracted from nested object |
| `fase_publicacio` + `resultat` | `status` | See status mapping below |
| `tipus_contracte` | `contract_nature`, `is_concession` | See type mapping below |
| `tipus_contracte` | `contract_type_original` | Used as-is (Catalan) |
| `procediment` | `procedure_type` | See procedure mapping below |
| `fase_publicacio` | `notice_type` | See notice mapping below |
| `objecte_contracte` | `contract_title` | Used as-is |
| `denominacio` | `lot_title` | Used as-is |
| `durada_contracte` | `contract_duration` | Used as-is (free text) |
| `nom_organ` | `contracting_authority` | Used as-is |
| `codi_organ` | `authority_id` | Used as-is |
| `nom_ambit` | `authority_type` | See authority mapping below |
| `codi_dir3` | `authority_dir3` | Used as-is |
| `lloc_execucio` | `place_of_execution` | Used as-is |
| `codi_nuts` | `nuts_code` | Used as-is |
| `valor_estimat_contracte` | `estimated_value` | Float |
| `pressupost_base_licitacio` | `base_budget` | Float |
| `denominacio_adjudicatari` | `awardee` | Used as-is |
| `identificacio_adjudicatari` | `awardee_id` | Used as-is |
| `import_adjudicacio_sense` | `award_value` | Float |
| `import_adjudicacio_amb_iva` | `award_value_with_tax` | Float |
| `ofertes_rebudes` | `num_offers` | Integer |
| `COALESCE(data_publicacio_anunci, data_publicacio_licitacio)` | `date_published` | First available |
| `GREATEST(data_publicacio_*)` | `date_updated` | Most recent of all publication dates |
| `data_adjudicacio_contracte` | `date_awarded` | Date only |
| `data_formalitzacio_contracte` | `date_contract_start` | Date only |
| `financament_europeu` | `eu_funded` | `'SI'` → true, `'NO'` → false |
| `es_agregada` | `is_aggregated` | `'SI'` → true, `'NO'` → false |
| `codi_cpv` | `cpv_codes` | Split on `\|\|` into array |
| — | `country` | Always `'ES'` |
| — | `*_currency` | Always `'EUR'` |

## Status Mapping

Priority: `resultat` > `fase_publicacio`

| `resultat` / `fase_publicacio` | `status` |
|-------------------------------|---------|
| resultat = `Desert` | `deserted` |
| resultat = `Desisitment` / `Desistiment` / `Renuncia` | `cancelled` |
| fase = `Anul·lacio` | `cancelled` |
| fase/resultat = `Formalitzacio` | `formalized` |
| fase/resultat = `Adjudicacio` | `awarded` |
| fase = `Expedient en avaluacio` | `evaluation` |
| fase = `Anunci previ` / `Alerta futura` / `Consulta preliminar del mercat` | `prior-notice` |
| fase = `Anunci de licitacio` | `announced` |
| fase = `Publicacio agregada de contractes` | `formalized` |

## Contract Type Mapping

| Source value (Catalan) | `contract_nature` | `is_concession` |
|-----------------------|------------------|-----------------|
| Obres | `works` | `false` |
| Serveis | `services` | `false` |
| Subministraments | `supplies` | `false` |
| Concessio de serveis | `services` | `true` |
| Concessio d'obres | `works` | `true` |
| Concessio de serveis especials (annex IV) | `services` | `true` |
| Contracte de serveis especials (annex IV) | `services` | `false` |
| Administratiu especial | `services` | `false` |
| Privat d'Administracio Publica | `services` | `false` |
| Altra legislacio sectorial | `services` | `false` |

## Procedure Type Mapping

| Source value (Catalan) | `procedure_type` |
|-----------------------|-----------------|
| Obert / Obert simplificat / Obert simplificat abreujat | `open` |
| Restringit | `restricted` |
| Negociat sense publicitat | `neg-wo-call` |
| Negociat amb publicitat / Licitacio amb negociacio | `neg-w-call` |
| Dialeg competitiu | `comp-dial` |
| Associacio per a la innovacio | `innovation` |
| Contracte menor | `minor` |
| Concurs de projectes | `design-contest` |
| Adjudicacions directes no menors / Altres procediments segons instruccions internes | `internal` |
| Tramitacio amb mesures de gestio eficient | `oth-single` |
| Especific de Sistema Dinamic d'adquisicio | `dynamic-acq` |

## Authority Type Mapping

| `nom_ambit` | `authority_type` |
|------------|-----------------|
| Departaments i sector public de la Generalitat de Catalunya | `ra` |
| Entitats de l'administracio local | `la` |
| Universitats | `university` |
| Organismes independents i/o estatutaris | `body-pl-ra` |
| Altres ens | `other` |

## Primary Key

`(source='ES_GENCAT', source_id, lot_number)`

- `source_id`: `codi_expedient + '/' + codi_organ` (e.g., `CTN2300100/206317`)
- `lot_number`: `numero_lot` or `'0'` if null

## Deduplication

Records with null `codi_expedient` or `codi_organ` are skipped. When multiple API records share the same `(codi_expedient, codi_organ, numero_lot)`, the Socrata `:updated_at` field ensures only the latest version is fetched within a date range. The upsert handles any remaining duplicates.
