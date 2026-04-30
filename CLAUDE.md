# EU Public Procurement Analysis

This project consolidates public procurement data from multiple EU sources into a unified data model. Currently includes Spain (ES_PLACE, ES_GENCAT) and France (FR_DECP).

The database schema is fully documented in [SCHEMA.md](SCHEMA.md).

## Database Connections

### PostgreSQL (Docker)

```bash
Host: localhost
Port: 5433
Database: tenders
User: tenders
Password: tenders

PGPASSWORD=tenders psql -h localhost -p 5433 -U tenders -d tenders
```

### BigQuery

```
Project: eu-spanish-tender-dataset
Dataset: EU_Spanish_Tender_Dataset
Region: US
```

```bash
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM EU_Spanish_Tender_Dataset.contracts"
```

## Project Structure

```
├── CLAUDE.md              # This file
├── SCHEMA.md              # Full database schema documentation
├── pyproject.toml         # Python project metadata and dependencies
├── compose.yml            # PostgreSQL container
├── .gitignore
├── schema/
│   ├── postgres/
│   │   ├── tenders.sql    # PostgreSQL DDL for tenders layer
│   │   └── assets.sql     # PostgreSQL DDL for assets/localities layer
│   └── bigquery/
│       ├── tenders.sql    # BigQuery DDL for tenders layer
│       └── assets.sql     # BigQuery DDL for assets/localities layer
├── source/
│   ├── es_place.py        # Incremental sync from ES_PLACE (ATOM/CODICE XML)
│   ├── es_gencat.py       # Incremental sync from ES_GENCAT (Socrata API)
│   ├── fr_decp.py         # Sync from FR_DECP (Parquet from data.gouv.fr)
│   └── pt_base.py         # Sync from PT_BASE (XLSX from dados.gov.pt)
└── docs/
    ├── es_place.md        # ES_PLACE source documentation and field mapping
    ├── es_gencat.md       # ES_GENCAT source documentation and field mapping
    ├── fr_decp.md         # FR_DECP source documentation and field mapping
    └── pt_base.md         # PT_BASE source documentation and field mapping
```

## Data Sources

| Source | Code | Coverage |
|--------|------|----------|
| Plataforma de Contratacion del Estado | `ES_PLACE` | All of Spain (except Catalunya-only tenders) |
| Plataforma de Contractacio Publica de Catalunya | `ES_GENCAT` | Catalunya |
| Donnees Essentielles de la Commande Publique | `FR_DECP` | France (awarded contracts only) |
| Portal BASE - Contratos Publicos | `PT_BASE` | Portugal (awarded contracts, 2012+) |

See `docs/` for detailed documentation on each source, including field mappings and data access methods.

## Data Model Summary

All source data is consolidated into a single `contracts` table with:
- **Primary key**: `(source, source_id, lot_number)`
- **EU-standard taxonomies** for contract nature, procedure type, authority type, notice type (based on [eForms SDK](https://github.com/OP-TED/eForms-SDK))
- **Concession flag** (`is_concession`) separate from contract nature, following EU Directives
- **CPV codes** as arrays for sector filtering
- **Tagging system** (`contracts_tags`) for flexible classification with manual override support
- **Company group mapping** (`companies`, `companies_groups`, `companies_groups_memberships`) for market analysis across JVs/UTEs

See [SCHEMA.md](SCHEMA.md) for full column definitions, source mappings, reference table contents, and auto-tagging rules.

## Common Queries

### Water sector contracts

```sql
-- All water-tagged contracts
SELECT c.*
FROM contracts c
JOIN contracts_tags t USING (source, source_id, lot_number)
WHERE t.tag = 'water';

-- Water concessions
SELECT c.*
FROM contracts c
JOIN contracts_tags t USING (source, source_id, lot_number)
WHERE t.tag = 'water' AND c.is_concession = TRUE;
```

### Search by company

```sql
SELECT source, contract_title, awardee, award_value, date_awarded
FROM contracts
WHERE awardee ILIKE '%aqualia%'
ORDER BY date_updated DESC;
```

### Company group market analysis

```sql
-- Weighted contract value by group (water sector)
SELECT g.name AS group_name,
       c.contract_nature,
       ROUND(SUM(
           COALESCE(NULLIF(c.estimated_value, 0), c.base_budget, 0)
           * (m.percentage / 100.0)
       ), 2) AS weighted_value
FROM contracts c
JOIN contracts_tags t USING (source, source_id, lot_number)
JOIN companies co ON c.awardee = co.name
JOIN companies_groups_memberships m ON co.id = m.company_id
JOIN companies_groups g ON m.group_id = g.id
WHERE t.tag = 'water'
GROUP BY g.name, c.contract_nature
ORDER BY weighted_value DESC;
```

### Add a company to a group

```sql
-- 1. Insert company
INSERT INTO companies (name) VALUES ('COMPANY NAME')
ON CONFLICT (name) DO NOTHING;

-- 2. Link to group
INSERT INTO companies_groups_memberships (company_id, group_id, percentage)
SELECT c.id, g.id, 100.00
FROM companies c, companies_groups g
WHERE c.name = 'COMPANY NAME' AND g.name = 'GROUP NAME';
```

### CPV code lookup

```sql
SELECT code, description FROM ref_cpv_codes
WHERE code LIKE '904%';
```

## Important Notes

1. **Value fallback**: Use `estimated_value` if > 0, otherwise fall back to `base_budget`.

2. **JV detection**: Company names with `-` separators indicate joint ventures (UTEs). Ownership percentage in `companies_groups_memberships` is split accordingly (50% for 2-party, 33.33% for 3-party, etc.).

3. **Incremental only**: All sync scripts must use upserts (`ON CONFLICT DO UPDATE` / `MERGE`). Never truncate, delete from, or drop production tables. Old data can be updated but never wiped. The database must never be emptied and rebuilt from scratch.

4. **Deduplication**: The `contracts` table stores only the latest state of each tender-lot. When syncing, use `MERGE`/upsert on the `(source, source_id, lot_number)` primary key.

5. **Character encoding**: Some company names from ES_PLACE have corrupted characters. Search with patterns to catch these.

6. **Espina Obras Hidraulicas** is NOT part of the "Espina y Delfin" group.

7. **SIMETRIA** was renamed from CICLAGUA. BECSA companies are part of this group.

8. **Partial ownership**: "Aguas Municipalizadas de Alicante" is 49% VEOLIA (minority stake).
