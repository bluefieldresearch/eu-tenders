# Database Schema

Unified EU public procurement data model. Consolidates multiple procurement data sources into a single structure with EU-standard taxonomies from the [eForms SDK](https://github.com/OP-TED/eForms-SDK).

Available in both PostgreSQL (Docker, `localhost:5433`, database `tenders`) and BigQuery (`eu-spanish-tender-dataset.EU_Spanish_Tender_Dataset`).

## Table Overview

| Table | Description | Records |
|-------|-------------|---------|
| `contracts` | Unified tender/lot records from all sources | ~2.77M |
| `contracts_tags` | Flexible tagging layer for contract classification | ~97K |
| `companies` | Company names as they appear in the `awardee` field | ~22K |
| `companies_groups` | Parent organizations / holding companies | 200 |
| `companies_groups_memberships` | Links companies to groups with ownership percentages | ~11K |
| `ref_contract_nature` | EU contract nature codes | 3 |
| `ref_procedure_type` | EU procurement procedure types | 13 |
| `ref_authority_type` | EU buyer/authority legal types | 18 |
| `ref_notice_type` | EU notice types | 20 |
| `ref_status` | Contract lifecycle status codes | 9 |
| `ref_country` | ISO 3166-1 alpha-2 country codes | 26 |
| `ref_currency` | ISO 4217 currency codes | 12 |
| `ref_source` | Data source platforms | 3 |
| `ref_cpv_codes` | Common Procurement Vocabulary (EU standard) | 9,454 |
| `ref_tags` | Tag definitions for contract classification | 4 |

---

## contracts

One row per tender-lot combination. Stores the latest state of each record (upsert on sync).

**Primary key:** `(source, source_id, lot_number)`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `source` | VARCHAR(20) | NO | | Data source platform. FK &rarr; `ref_source.code` |
| `source_id` | VARCHAR(200) | NO | | Platform-native identifier. PLACE: `identificador` (bigint as text). GENCAT: `codi_expedient/codi_organ` |
| `lot_number` | VARCHAR(200) | NO | `'0'` | Lot identifier. `'0'` for single-lot tenders |
| `reference_number` | VARCHAR(200) | YES | | Human-readable reference (n&uacute;mero de expediente) |
| `source_url` | TEXT | YES | | Link to the original tender on the source platform |
| `status` | VARCHAR(20) | YES | | Lifecycle status. FK &rarr; `ref_status.code` |
| `contract_nature` | VARCHAR(20) | YES | | EU contract nature. FK &rarr; `ref_contract_nature.code` |
| `is_concession` | BOOLEAN | YES | `false` | Whether the contract is a concession (Concessions Directive 2014/23/EU) |
| `contract_type_original` | VARCHAR(200) | YES | | Raw contract type from the source platform, preserved as-is |
| `procedure_type` | VARCHAR(30) | YES | | Procurement procedure. FK &rarr; `ref_procedure_type.code` |
| `notice_type` | VARCHAR(30) | YES | | Type of notice/publication. FK &rarr; `ref_notice_type.code` |
| `contract_title` | TEXT | YES | | Contract title / subject (objeto del contrato) |
| `lot_title` | TEXT | YES | | Lot-specific title, if different from contract title |
| `contract_duration` | VARCHAR(200) | YES | | Contract duration as free text (from GENCAT) |
| `contracting_authority` | TEXT | YES | | Name of the contracting authority (&oacute;rgano de contrataci&oacute;n) |
| `authority_id` | VARCHAR(50) | YES | | Identifier of the authority (NIF for PLACE, codi_organ for GENCAT) |
| `authority_type` | VARCHAR(30) | YES | | Type of authority. FK &rarr; `ref_authority_type.code` |
| `authority_dir3` | VARCHAR(50) | YES | | DIR3 code (Spanish public administration directory) |
| `place_of_execution` | VARCHAR(500) | YES | | Place of execution / delivery |
| `nuts_code` | VARCHAR(20) | YES | | NUTS code for location (from GENCAT) |
| `country` | VARCHAR(5) | YES | | Country code. FK &rarr; `ref_country.code` |
| `estimated_value` | DECIMAL(18,2) | YES | | Estimated contract/lot value (excl. tax) |
| `estimated_value_currency` | VARCHAR(5) | YES | `'EUR'` | FK &rarr; `ref_currency.code` |
| `base_budget` | DECIMAL(18,2) | YES | | Base budget without taxes |
| `base_budget_currency` | VARCHAR(5) | YES | `'EUR'` | FK &rarr; `ref_currency.code` |
| `awardee` | TEXT | YES | | Awarded company name, verbatim from source. Joins to `companies.name` |
| `awardee_id` | TEXT | YES | | Awardee tax ID (NIF/CIF) when available |
| `award_value` | DECIMAL(18,2) | YES | | Award amount without taxes |
| `award_value_with_tax` | DECIMAL(18,2) | YES | | Award amount including tax (from GENCAT) |
| `award_value_currency` | VARCHAR(5) | YES | `'EUR'` | FK &rarr; `ref_currency.code` |
| `num_offers` | INTEGER | YES | | Number of offers/bids received |
| `excluded_low_offers` | BOOLEAN | YES | | Whether abnormally low offers were excluded (PLACE only) |
| `date_published` | TIMESTAMP | YES | | Date the tender was first published |
| `date_updated` | TIMESTAMP | YES | | Date of the most recent update from the source |
| `date_awarded` | DATE | YES | | Date the contract was awarded |
| `date_contract_start` | DATE | YES | | Date the contract entered into force |
| `eu_funded` | BOOLEAN | YES | | Whether the project has EU funding (from GENCAT) |
| `is_aggregated` | BOOLEAN | YES | | Whether this is part of an aggregated minor contracts publication (GENCAT) |
| `cpv_codes` | TEXT[] | YES | | Array of CPV codes. FK &rarr; `ref_cpv_codes.code` |
| `last_synced_at` | TIMESTAMP | YES | `now()` | When this record was last synced from the source |

### Source-specific mapping

| Field | PLACE source | GENCAT source |
|-------|-------------|---------------|
| `source_id` | `CAST(identificador AS TEXT)` | `codi_expedient \|\| '/' \|\| codi_organ` |
| `lot_number` | `resultados.lote` | `numero_lot` |
| `reference_number` | `numero_expediente` | `codi_expedient` |
| `source_url` | `link_licitacion` | `enllac_publicacio` |
| `contract_title` | `objeto_contrato` | `objecte_contracte` |
| `lot_title` | `resultados.objeto_lote` | `denominacio` |
| `contracting_authority` | `organo_contratacion` | `nom_organ` |
| `authority_id` | `nif_oc` | `codi_organ` |
| `estimated_value` | `resultados.valor_estimado_lote` | `valor_estimat_contracte` |
| `base_budget` | `resultados.presupuesto_base_sin_impuestos_lote` | `pressupost_base_licitacio` |
| `awardee` | `resultados.adjudicatario` | `denominacio_adjudicatari` |
| `award_value` | `resultados.importe_adjudicacion_sin_impuestos` | `import_adjudicacio_sense` |
| `procedure_type` | not available | mapped from `procediment` |
| `nuts_code` | not available | `codi_nuts` |
| `contract_duration` | not available | `durada_contracte` |
| `eu_funded` | not available | `finançament_europeu` |

### Deduplication

When multiple records exist for the same `(source, source_id, lot_number)`, only the most recent is kept:

- **PLACE:** `ORDER BY fecha_actualizacion DESC` (same tender appears across multiple year files)
- **GENCAT:** `ORDER BY GREATEST(data_publicacio_*) DESC` (same tender updated through lifecycle)

---

## contracts_tags

Flexible tagging layer. A contract can have multiple tags. Supports both automatic (rule-based) and manual tagging. Manual overrides are preserved across auto-tag runs via `ON CONFLICT DO NOTHING`.

**Primary key:** `(source, source_id, lot_number, tag)`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `source` | VARCHAR(20) | NO | | FK &rarr; `contracts` |
| `source_id` | VARCHAR(200) | NO | | FK &rarr; `contracts` |
| `lot_number` | VARCHAR(200) | NO | | FK &rarr; `contracts` |
| `tag` | VARCHAR(100) | NO | | Tag code. FK &rarr; `ref_tags.code` |
| `auto_tagged` | BOOLEAN | YES | `false` | Set by automatic classification rules |
| `manually_set` | BOOLEAN | YES | `false` | Set or confirmed by a human |
| `tagged_at` | TIMESTAMP | YES | `now()` | When the tag was applied |

### Auto-tagging rules

**`water` tag (PLACE):** CPV code matching — includes codes `651%`, `655%`, `904%`, `45231%`, `45232%`, `45240%`, `45247%`, `45248%`, `45252%`, `45253%`, `45259%`, `45262220`, `4416%`, `44611%`, `44613500`, `4212%`, `38421%`, `50514%`, `7163%`, `90713%`, `90733%`, `90913%`. Excludes `4523214%`, `4523222%`, `4523223%`, `45232470`.

**`water` tag (GENCAT):** Keyword matching on `contract_title` (`%aigua%`, `%abastament%`, `%sanejament%`, `%depuradora%`, `%EDAR%`, `%ETAP%`, etc.) and `contracting_authority` (`%aigua%`, `%ACA%`, `%ATL%`).

---

## companies

Company names as they appear in the `awardee` field of `contracts`. Used to link raw award data to company groups.

**Primary key:** `id` (serial). **Unique:** `name`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | SERIAL | NO | Auto-increment ID |
| `name` | VARCHAR(500) | NO | Company name exactly as it appears in source data |
| `notes` | TEXT | YES | Free-text notes |

---

## companies_groups

Parent organizations or holding companies that own or control multiple companies.

**Primary key:** `id` (serial). **Unique:** `name`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | SERIAL | NO | Auto-increment ID |
| `name` | VARCHAR(500) | NO | Group name (e.g., `VEOLIA`, `FCC AQUALIA`) |
| `description` | TEXT | YES | Description of the group |
| `sector` | VARCHAR(500) | YES | Industry sector |

---

## companies_groups_memberships

Links companies to groups with ownership percentages. Handles joint ventures (UTEs) where a single company name maps to multiple groups with fractional ownership.

**Primary key:** `id` (serial). **Foreign keys:** `company_id` &rarr; `companies.id`, `group_id` &rarr; `companies_groups.id`.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | SERIAL | NO | | Auto-increment ID |
| `company_id` | INTEGER | NO | | FK &rarr; `companies.id` |
| `group_id` | INTEGER | NO | | FK &rarr; `companies_groups.id` |
| `percentage` | DECIMAL(5,2) | YES | `100.00` | Ownership percentage. 100 = pure entity, 50 = 2-party JV, 33.33 = 3-party JV |
| `notes` | TEXT | YES | | Free-text notes |

### Percentage rules

- **100%**: Pure company (e.g., `FCC AQUALIA, S.A.`)
- **50%**: 2-party JV/UTE (e.g., `UTE FACSA - ACCIONA`)
- **33.33%**: 3-party JV (e.g., `UTE FACSA - ISLASFALTO - EXCAVACIONES BUFI`)
- **25%**: 4-party JV
- **Special cases**: Some companies have specific percentages (e.g., `Aguas Municipalizadas de Alicante` is 49% VEOLIA)

JV detection: company names with `-` separators indicate joint ventures. Count separators to determine number of parties.

---

## Reference Tables

All reference tables use `code` as the primary key. Values are based on the [EU eForms SDK](https://github.com/OP-TED/eForms-SDK) with extensions for source-specific values.

### ref_contract_nature

EU standard contract nature ([eForms contract-nature](https://github.com/OP-TED/eForms-SDK/blob/main/codelists/contract-nature.gc)). Concessions are **not** a separate nature — they are flagged via `contracts.is_concession`.

| Code | Label |
|------|-------|
| `works` | Works |
| `services` | Services |
| `supplies` | Supplies |

#### Mapping from source values

| Source value (PLACE) | `contract_nature` | `is_concession` |
|---------------------|-------------------|-----------------|
| Obras | `works` | `false` |
| Servicios | `services` | `false` |
| Suministros | `supplies` | `false` |
| Concesion de Servicios | `services` | `true` |
| Concesion de Obras | `works` | `true` |
| Concesion de Obras Publicas | `works` | `true` |
| Gestion de Servicios Publicos | `services` | `true` |
| Colaboracion entre el sector publico y sector privado | `services` | `false` |
| Administrativo especial | `services` | `false` |
| Privado | `services` | `false` |
| Patrimonial | `NULL` | `false` |

| Source value (GENCAT) | `contract_nature` | `is_concession` |
|----------------------|-------------------|-----------------|
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

### ref_procedure_type

Procurement procedure types. Based on [eForms procurement-procedure-type](https://github.com/OP-TED/eForms-SDK/blob/main/codelists/procurement-procedure-type.gc) with extensions for `minor`, `internal`, `design-contest`, and `dynamic-acq`.

| Code | Label |
|------|-------|
| `open` | Open |
| `restricted` | Restricted |
| `neg-w-call` | Negotiated with prior publication of a call for competition |
| `neg-wo-call` | Negotiated without prior call for competition |
| `comp-dial` | Competitive dialogue |
| `innovation` | Innovation partnership |
| `comp-tend` | Competitive tendering |
| `oth-single` | Other single stage procedure |
| `oth-mult` | Other multiple stage procedure |
| `minor` | Minor contract |
| `internal` | Internal procedure |
| `design-contest` | Design contest |
| `dynamic-acq` | Dynamic acquisition system |

### ref_authority_type

Buyer/authority legal types. Based on [eForms buyer-legal-type](https://github.com/OP-TED/eForms-SDK/blob/main/codelists/buyer-legal-type.gc) with extensions for `university` and `other`.

| Code | Label |
|------|-------|
| `cga` | Central government authority |
| `ra` | Regional authority |
| `la` | Local authority |
| `body-pl` | Body governed by public law |
| `body-pl-cga` | Body governed by public law, controlled by a central government authority |
| `body-pl-la` | Body governed by public law, controlled by a local authority |
| `body-pl-ra` | Body governed by public law, controlled by a regional authority |
| `pub-undert` | Public undertaking |
| `pub-undert-cga` | Public undertaking, controlled by a central government authority |
| `pub-undert-la` | Public undertaking, controlled by a local authority |
| `pub-undert-ra` | Public undertaking, controlled by a regional authority |
| `org-sub` | Organisation awarding a contract subsidised by a contracting authority |
| `eu-ins-bod-ag` | EU institution, body or agency |
| `int-org` | International organisation |
| `def-cont` | Defence contractor |
| `spec-rights-entity` | Entity with special or exclusive rights |
| `university` | University |
| `other` | Other |

### ref_notice_type

Notice/publication types. Based on [eForms notice-type](https://github.com/OP-TED/eForms-SDK/blob/main/codelists/notice-type.gc) with extension for `aggregated`.

| Code | Label |
|------|-------|
| `cn-standard` | Contract or concession notice — standard regime |
| `cn-social` | Contract notice — light regime |
| `cn-desg` | Design contest notice |
| `can-standard` | Contract or concession award notice — standard regime |
| `can-social` | Contract or concession award notice — light regime |
| `can-modif` | Contract modification notice |
| `can-desg` | Design contest result notice |
| `can-tran` | Contract award notice for public passenger transport services |
| `pin-only` | Prior information notice used only for information |
| `pin-cfc-standard` | Prior information notice used as call for competition — standard regime |
| `pin-cfc-social` | Prior information notice used as call for competition — light regime |
| `pin-buyer` | Notice of publication of a prior information notice on a buyer profile |
| `pin-rtl` | Prior information notice used to shorten time limits |
| `pin-tran` | Prior information notice for public passenger transport services |
| `compl` | Contract completion notice |
| `veat` | Voluntary ex-ante transparency notice |
| `subco` | Subcontracting notice |
| `pmc` | Pre-market consultation notice |
| `qu-sy` | Notice on the existence of a qualification system |
| `aggregated` | Aggregated minor contracts publication |

### ref_status

Contract lifecycle status codes (custom taxonomy).

| Code | Label | PLACE mapping | GENCAT mapping |
|------|-------|--------------|----------------|
| `announced` | Announced | `VIGENTE` (no result) | `Anunci de licitacio` |
| `evaluation` | Under evaluation | — | `Expedient en avaluacio` |
| `awarded` | Awarded | `VIGENTE` (with result, no fecha_entrada_vigor) | `Adjudicacio` |
| `formalized` | Formalized | `VIGENTE` (with result + fecha_entrada_vigor) | `Formalitzacio`, aggregated contracts |
| `cancelled` | Cancelled | `ANULADA` | `Anul·lacio`, `Desisitment`, `Renuncia` |
| `deserted` | Deserted | — | `Desert` |
| `archived` | Archived | — | — |
| `modified` | Modified | — | — |
| `prior-notice` | Prior information notice | — | `Anunci previ`, `Alerta futura`, `Consulta preliminar del mercat` |

### ref_country

ISO 3166-1 alpha-2 codes. 26 EU member states pre-loaded.

### ref_currency

ISO 4217 codes. Includes EUR, SEK, DKK, PLN, CZK, HUF, RON, BGN, HRK, GBP, NOK, CHF.

### ref_source

| Code | Label | URL | Country |
|------|-------|-----|---------|
| `PLACE` | Plataforma de Contratacion del Estado | https://contrataciondelestado.es | ES |
| `GENCAT` | Plataforma de Contractacio Publica de Catalunya | https://contractaciopublica.cat | ES |
| `TED` | Tenders Electronic Daily | https://ted.europa.eu | — |

### ref_cpv_codes

Common Procurement Vocabulary (EU standard, 2008 edition). 9,454 codes with English descriptions sourced from the [eForms SDK](https://github.com/OP-TED/eForms-SDK/blob/main/codelists/cpv.gc).

| Column | Type | Description |
|--------|------|-------------|
| `code` | VARCHAR(10) | CPV code (e.g., `90400000`) |
| `description` | TEXT | English description (e.g., `Sewage services`) |

### ref_tags

Tags for contract classification.

| Code | Label | Description |
|------|-------|-------------|
| `water` | Water & Wastewater | Water supply, wastewater treatment, drainage, irrigation |
| `water-supply` | Water Supply | Drinking water production and distribution |
| `wastewater` | Wastewater | Wastewater collection and treatment |
| `desalination` | Desalination | Seawater and brackish water desalination |

---

## Entity-Relationship Diagram

```
contracts (source, source_id, lot_number)
    ├── FK → ref_source (source)
    ├── FK → ref_status (status)
    ├── FK → ref_contract_nature (contract_nature)
    ├── FK → ref_procedure_type (procedure_type)
    ├── FK → ref_notice_type (notice_type)
    ├── FK → ref_authority_type (authority_type)
    ├── FK → ref_country (country)
    ├── FK → ref_currency (estimated_value_currency, base_budget_currency, award_value_currency)
    ├── cpv_codes[] → ref_cpv_codes (code)
    ├── awardee → companies (name)
    └── contracts_tags (source, source_id, lot_number, tag)
            └── FK → ref_tags (tag)

companies (id)
    └── companies_groups_memberships (company_id, group_id)
            ├── FK → companies (company_id)
            └── FK → companies_groups (group_id)
```

## Indexes

### contracts
- `idx_contracts_source` — `(source)`
- `idx_contracts_status` — `(status)`
- `idx_contracts_nature` — `(contract_nature)`
- `idx_contracts_concession` — `(is_concession)`
- `idx_contracts_procedure` — `(procedure_type)`
- `idx_contracts_authority` — `(contracting_authority)`
- `idx_contracts_authority_type` — `(authority_type)`
- `idx_contracts_country` — `(country)`
- `idx_contracts_place` — `(place_of_execution)`
- `idx_contracts_awardee` — `(awardee)`
- `idx_contracts_date_published` — `(date_published)`
- `idx_contracts_date_updated` — `(date_updated)`
- `idx_contracts_date_awarded` — `(date_awarded)`
- `idx_contracts_reference` — `(reference_number)`
- `idx_contracts_cpv` — GIN on `(cpv_codes)`
- `idx_contracts_title_fts` — GIN full-text on `contract_title`
- `idx_contracts_awardee_fts` — GIN full-text on `awardee`

### contracts_tags
- `idx_contracts_tags_tag` — `(tag)`
- `idx_contracts_tags_auto` — `(auto_tagged)`
- `idx_contracts_tags_manual` — `(manually_set)`

### companies
- `idx_companies_name` — `(name)`

### companies_groups_memberships
- `idx_companies_groups_memberships_company` — `(company_id)`
- `idx_companies_groups_memberships_group` — `(group_id)`
