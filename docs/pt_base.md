# PT_BASE — Portal BASE (Contratos Publicos)

## Overview

Portugal's central public procurement platform, managed by IMPIC (Instituto dos Mercados Publicos, do Imobiliario e da Construcao). Publishes awarded contracts from all levels of Portuguese public administration.

- **Website**: https://www.base.gov.pt
- **Country**: Portugal
- **Coverage**: All Portuguese public bodies — central government, municipalities, public enterprises, hospitals
- **Data format**: XLSX (yearly files on dados.gov.pt)
- **Update frequency**: Files updated periodically on dados.gov.pt
- **License**: Public domain
- **Data range**: 2007 to present (2012+ in structured XLSX format)

## Data Access

Yearly XLSX files are published on Portugal's open data portal:

https://dados.gov.pt/en/datasets/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/

File naming pattern: `contratos{YYYY}.xlsx`

The download URLs are resolved dynamically via the dados.gov.pt API since they include date-based path prefixes that change with each update:

```
GET https://dados.gov.pt/api/1/datasets/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/
```

Additional datasets available (not currently imported):
- **Anuncios** (tender announcements): https://dados.gov.pt/en/datasets/contratos-publicos-portal-base-impic-anuncios-de-2012-a-2026/
- **Modificacoes** (contract amendments): https://dados.gov.pt/en/datasets/contratos-publicos-portal-base-impic-modificacoes-contratuais-de-2012-a-2025/

An official IMPIC API also exists but requires registration.

## Sync Script

```bash
python3 source/pt_base.py --from 2024-01-01 --to 2024-12-31
python3 source/pt_base.py --from 2025-01-01  # defaults --to to today
```

The script derives which yearly files to download from the date range, reads each XLSX, filters by `dataPublicacao`, and upserts into the `contracts` table.

**Important**: Like FR_DECP, this source downloads full yearly files and filters locally. The `--from`/`--to` parameters filter by publication date after download.

## Data Characteristics

- **Only awarded contracts**: BASE publishes contracts after award/signing. No pre-award tender announcements (those are in the separate Anuncios dataset).
- **No lot-level data**: Each row is one contract. `lot_number` is always `'0'`.
- **Buyer/awardee format**: NIF and name are combined in a single field as `NIF - Name` (e.g., `506579425 - Municipio de Faro`).
- **Multiple awardees**: Some contracts have multiple awardees (newline-separated). These are joined with ` | `.
- **CPV codes**: Include descriptions (e.g., `50750000-7 - Servicos de manutencao de elevadores`). Multiple codes are newline-separated.
- **Bidder count**: Derived from the `concorrentes` field (count of newline-separated entries).

## Source Fields

| Field | Type | Description |
|-------|------|-------------|
| `idcontrato` | integer | Unique contract identifier |
| `nAnuncio` | string | Notice number (often empty) |
| `tipoContrato` | string | Contract type(s), newline-separated if multiple |
| `idprocedimento` | integer | Procedure identifier |
| `tipoprocedimento` | string | Procurement procedure |
| `objectoContrato` | string | Contract subject (full description) |
| `descContrato` | string | Contract description |
| `adjudicante` | string | Contracting authority (`NIF - Name`) |
| `adjudicatarios` | string | Awardee(s) (`NIF - Name`, newline-separated) |
| `dataPublicacao` | datetime | Publication date on BASE |
| `dataCelebracaoContrato` | datetime | Contract signing date |
| `dataDecisaoAdjudicacao` | datetime | Award decision date |
| `dataFechoContrato` | datetime | Contract closing date |
| `precoContratual` | number | Contract price (award value) |
| `precoBaseProcedimento` | number | Base procedure price (budget) |
| `PrecoTotalEfetivo` | number | Actual total price |
| `CPV` | string | CPV codes with descriptions (newline-separated) |
| `prazoExecucao` | integer | Execution period in days |
| `LocalExecucao` | string | Place of execution |
| `fundamentacao` | string | Legal basis |
| `ProcedimentoCentralizado` | string | Whether centralized (Sim/Nao) |
| `regime` | string | Legal regime |
| `concorrentes` | string | Bidders (`NIF-Name`, newline-separated) |
| `linkPecasProc` | string | Link to procedure documents |
| `ContratEcologico` | string | Ecological contract flag |
| `Ano` | integer | Year |

## Field Mapping

| Source field | contracts column | Transformation |
|-------------|-----------------|----------------|
| `idcontrato` | `source_id` | String |
| `idprocedimento` | `reference_number` | String |
| — | `source_url` | Constructed: `https://www.base.gov.pt/Base4/pt/detalhe/?type=contratos&id={idcontrato}` |
| — | `status` | Always `'formalized'` (BASE only has awarded contracts) |
| `tipoContrato` (first value) | `contract_nature`, `is_concession` | See type mapping below |
| `tipoContrato` | `contract_type_original` | All types joined with `; ` |
| `tipoprocedimento` | `procedure_type` | See procedure mapping below |
| — | `notice_type` | Always `'can-standard'` |
| `objectoContrato` | `contract_title` | Used as-is |
| `prazoExecucao` | `contract_duration` | `"{days} days"` |
| `adjudicante` (name part) | `contracting_authority` | Parsed from `NIF - Name` |
| `adjudicante` (NIF part) | `authority_id` | Parsed from `NIF - Name` |
| `LocalExecucao` | `place_of_execution` | Used as-is |
| `precoBaseProcedimento` | `base_budget` | Float |
| `adjudicatarios` (names) | `awardee` | Parsed, joined with ` \| ` if multiple |
| `adjudicatarios` (NIFs) | `awardee_id` | Parsed, joined with ` \| ` if multiple |
| `precoContratual` | `award_value` | Float |
| `concorrentes` | `num_offers` | Count of newline-separated entries |
| `dataPublicacao` | `date_published`, `date_updated` | Timestamp |
| `dataDecisaoAdjudicacao` | `date_awarded` | Date |
| `dataCelebracaoContrato` | `date_contract_start` | Date |
| `CPV` | `cpv_codes` | Codes extracted from `CODE - Description` format |
| — | `country` | Always `'PT'` |
| — | `*_currency` | Always `'EUR'` |

## Contract Type Mapping

| Source value (Portuguese) | `contract_nature` | `is_concession` |
|--------------------------|------------------|-----------------|
| Aquisicao de bens moveis | `supplies` | `false` |
| Aquisicao de servicos | `services` | `false` |
| Empreitadas de obras publicas | `works` | `false` |
| Locacao de bens moveis | `supplies` | `false` |
| Concessao de servicos publicos | `services` | `true` |
| Concessao de obras publicas | `works` | `true` |
| Sociedade | `services` | `false` |
| Outros | `NULL` | `false` |

When a contract has multiple types (newline-separated), the first type is used for mapping.

## Procedure Type Mapping

| Source value (Portuguese) | `procedure_type` |
|--------------------------|-----------------|
| Ajuste Direto Regime Geral | `neg-wo-call` |
| Ajuste direto simplificado | `neg-wo-call` |
| Consulta Previa | `neg-w-call` |
| Consulta Previa Simplificada | `neg-w-call` |
| Concurso publico | `open` |
| Concurso publico simplificado | `open` |
| Concurso limitado por previa qualificacao | `restricted` |
| Procedimento de negociacao | `neg-w-call` |
| Ao abrigo de acordo-quadro | `oth-single` |
| Contratacao excluida II | `oth-single` |
| Setores especiais | `oth-single` |
| Concurso de concecao simplificado | `design-contest` |

## Primary Key

`(source='PT_BASE', source_id, lot_number)`

- `source_id`: The `idcontrato` field (e.g., `10400194`)
- `lot_number`: Always `'0'` (no lot-level granularity)

## Deduplication

Each `idcontrato` is unique in the source data. The upsert handles re-imports of the same year file gracefully.
