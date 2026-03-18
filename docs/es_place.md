# ES_PLACE — Plataforma de Contratación del Estado

## Overview

Spain's national public procurement platform, managed by the Ministry of Finance. Publishes tenders and award results from all levels of Spanish public administration (central, regional, local).

- **Website**: https://contrataciondelestado.es
- **Country**: Spain
- **Coverage**: All of Spain (except Catalunya-only tenders published exclusively on GENCAT)
- **Data format**: ATOM/CODICE XML (monthly ZIP archives)
- **Update frequency**: Daily snapshots within monthly archives

## Data Access

Monthly ZIP archives are published at:

```
https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_YYYYMM.zip
```

Annual archives are also available:

```
https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_YYYY.zip
```

Download page: https://www.hacienda.gob.es/es-es/gobiernoabierto/datos%20abiertos/paginas/licitacionescontratante.aspx

Each ZIP contains multiple ATOM XML files — one set per day, split into ~500-entry parts. Files follow the naming pattern:

```
licitacionesPerfilesContratanteCompleto3_YYYYMMDD_HHMMSS.atom      # first part
licitacionesPerfilesContratanteCompleto3_YYYYMMDD_HHMMSS_1.atom    # second part
...
```

## Sync Script

```bash
python3 source/es_place.py --from 2026-01-01 --to 2026-03-18
python3 source/es_place.py --from 2026-03-01  # defaults --to to today
```

The script downloads monthly ZIPs covering the date range, parses all ATOM entries, filters by `<updated>` date, and upserts into the `contracts` table.

## XML Schema

Each entry uses the CODICE (Componentes y Documentos Interoperables para la Contratación Electrónica) XML format, an extension of UBL. Key XML elements:

| XML Path | Description |
|----------|-------------|
| `atom:id` | Unique entry identifier (URL) |
| `atom:updated` | Last modification timestamp |
| `atom:link@href` | Link to tender detail page |
| `ContractFolderID` | Reference number (numero de expediente) |
| `ContractFolderStatusCode` | Status code (PUB, EV, ADJ, RES, ANUL, PRE) |
| `ProcurementProject/Name` | Contract title |
| `ProcurementProject/TypeCode` | Contract type code |
| `ProcurementProject/BudgetAmount/EstimatedOverallContractAmount` | Estimated value |
| `ProcurementProject/BudgetAmount/TaxExclusiveAmount` | Base budget (excl. tax) |
| `RequiredCommodityClassification/ItemClassificationCode` | CPV code |
| `RealizedLocation/CountrySubentityCode` | NUTS code |
| `PlannedPeriod/DurationMeasure` | Contract duration |
| `LocatedContractingParty/Party/PartyName/Name` | Contracting authority name |
| `Party/PartyIdentification/ID[@schemeName='NIF']` | Authority NIF |
| `Party/PartyIdentification/ID[@schemeName='DIR3']` | DIR3 code |
| `ContractingPartyTypeCode` | Authority type code |
| `TenderResult/ResultCode` | Award result code |
| `TenderResult/ReceivedTenderQuantity` | Number of offers |
| `TenderResult/WinningParty/PartyName/Name` | Awardee name |
| `TenderResult/WinningParty/PartyIdentification/ID` | Awardee NIF |
| `TenderResult/AwardedTenderedProject/LegalMonetaryTotal/TaxExclusiveAmount` | Award value |
| `TenderResult/Contract/IssueDate` | Contract start date |
| `ProcurementProjectLot/ID` | Lot number |
| `ProcurementProjectLot/ProcurementProject/Name` | Lot title |

## Field Mapping

| Source field | contracts column | Transformation |
|-------------|-----------------|----------------|
| `atom:id` | `source_id` | Used as-is (URL string) |
| `ContractFolderID` | `reference_number` | Used as-is |
| `atom:link@href` | `source_url` | Used as-is |
| `ContractFolderStatusCode` | `status` | See status mapping below |
| `ProcurementProject/Name` | `contract_title` | Used as-is |
| `TypeCode` | `contract_nature`, `is_concession` | See type mapping below |
| `TypeCode` | `contract_type_original` | Mapped to Spanish name |
| `EstimatedOverallContractAmount` | `estimated_value` | Float |
| `TaxExclusiveAmount` | `base_budget` | Float |
| `ItemClassificationCode` | `cpv_codes` | Array with all CPV codes from entry |
| `CountrySubentityCode` | `nuts_code` | Used as-is |
| `DurationMeasure` | `contract_duration` | `"{value} {unitCode}"` |
| `Party/Name` | `contracting_authority` | Used as-is |
| `ID[@schemeName='NIF']` | `authority_id` | Used as-is |
| `ID[@schemeName='DIR3']` | `authority_dir3` | Used as-is |
| `ContractingPartyTypeCode` | `authority_type` | See authority mapping below |
| `atom:updated` | `date_updated` | Trimmed to `YYYY-MM-DD HH:MM:SS` |
| `ResultCode` | `status` | Overrides folder status when results exist |
| `ReceivedTenderQuantity` | `num_offers` | Integer |
| `WinningParty/Name` | `awardee` | Used as-is |
| `WinningParty/ID` | `awardee_id` | NIF of awardee |
| `LegalMonetaryTotal/TaxExclusiveAmount` | `award_value` | Float |
| `Contract/IssueDate` | `date_contract_start` | Date |
| `ProcurementProjectLot/ID` | `lot_number` | Used as-is, `'0'` if no lots |
| `ProcurementProjectLot/Name` | `lot_title` | Used as-is |
| — | `country` | Always `'ES'` |
| — | `*_currency` | Always `'EUR'` |

## Status Mapping

| StatusCode | `status` |
|-----------|---------|
| `PUB` | `announced` |
| `EV` | `evaluation` |
| `ADJ` | `awarded` |
| `RES` | `formalized` |
| `ANUL` | `cancelled` |
| `PRE` | `prior-notice` |

When `TenderResult` is present, `ResultCode` overrides:

| ResultCode | `status` |
|-----------|---------|
| 3 | `awarded` |
| 4 | `deserted` |
| 5, 6, 7 | `cancelled` |
| 8, 9 | `formalized` |

## Contract Type Mapping

| TypeCode | `contract_nature` | `is_concession` | Original name |
|----------|------------------|-----------------|---------------|
| 1 | `supplies` | `false` | Suministros |
| 2 | `services` | `false` | Servicios |
| 3 | `works` | `false` | Obras |
| 7 | `services` | `true` | Gestion de Servicios Publicos |
| 8 | `services` | `true` | Concesion de Servicios |
| 21 | `works` | `true` | Concesion de Obras Publicas |
| 22 | `works` | `true` | Concesion de Obras |
| 31 | `services` | `false` | Administrativo especial |
| 40 | `services` | `false` | Colaboracion publico-privado |
| 50 | `services` | `false` | Privado |
| 99 | `NULL` | `false` | Patrimonial |

## Authority Type Mapping

| TypeCode | `authority_type` | Description |
|----------|-----------------|-------------|
| 1 | `cga` | Administracion General del Estado |
| 2 | `ra` | Comunidad Autonoma |
| 3 | `la` | Entidad Local |
| 4 | `body-pl` | Organismo Autonomo |
| 5 | `body-pl-cga` | Entidad de Derecho Publico (estatal) |
| 6 | `pub-undert-cga` | Sociedad mercantil publica (estatal) |
| 7 | `body-pl-ra` | Entidad de Derecho Publico (autonomica) |
| 8 | `pub-undert-ra` | Sociedad mercantil (autonomica) |
| 9 | `body-pl-la` | Entidad de Derecho Publico (local) |
| 10 | `pub-undert-la` | Sociedad mercantil (local) |
| 11 | `other` | Otras entidades del sector publico |
| 12 | `university` | Universidad |

## Primary Key

`(source='ES_PLACE', source_id, lot_number)`

- `source_id`: The `atom:id` URL (e.g., `https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/19140258`)
- `lot_number`: From `ProcurementProjectLot/ID`, or `'0'` for single-lot tenders

## Deduplication

The same tender may appear across multiple daily snapshots and monthly archives. The upsert (`ON CONFLICT DO UPDATE`) ensures only the latest version is kept.
