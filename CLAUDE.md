# Spanish Public Tender Analysis - Water & Wastewater Sector

This project analyzes Spanish public procurement data (licitaciones) with a focus on the water and wastewater sector. Data is imported from Excel files published by the Spanish government and stored in PostgreSQL for analysis.

## Database Connection

```bash
# PostgreSQL in Docker container
Host: localhost
Port: 5433
Database: licitaciones
User: licitaciones
Password: licitaciones

# Connection command
PGPASSWORD=licitaciones psql -h localhost -p 5433 -U licitaciones -d licitaciones
```

## Project Structure

```
/home/delolmopro/Desktop/licitaciones/
├── CLAUDE.md                 # This file
├── schema.sql                # Base database schema
├── import_data.py            # Python script to import Excel files
├── queries/                  # Saved SQL queries
│   ├── water_wastewater_tenders.sql
│   ├── adjudicatarios_by_tipo_contrato.sql
│   ├── search_by_adjudicatario.sql
│   └── company_group_by_year_tipo.sql
├── exports/                  # CSV exports
│   ├── company_groups_by_year_tipo.csv
│   └── fcc_aqualia_by_year_tipo.csv
├── resources/                # Raw ATOM feed data (historical)
└── *.xlsx                    # Source Excel files (2017-2025)
```

## Database Schema

### Core Tables (from schema.sql)

| Table | Description | Records |
|-------|-------------|---------|
| `licitaciones` | Main tender announcements | ~1.2M |
| `resultados` | Award results (can have multiple per tender/lot) | ~1.6M |
| `licitaciones_cpv` | CPV codes linked to tenders | |
| `resultados_cpv` | CPV codes linked to results | |
| `cpv_codes` | Reference table for CPV codes | |

### Company Group Tables (created manually)

```sql
-- Company groups (parent organizations)
CREATE TABLE company_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE,
    description TEXT
);

-- Companies (as they appear in adjudicatario field)
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE,
    notes TEXT
);

-- Membership with ownership percentage
CREATE TABLE company_group_memberships (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES company_groups(id) ON DELETE CASCADE,
    percentage DECIMAL(5,2) DEFAULT 100.00,
    notes TEXT
);
```

## Company Groups (20 groups, 822 memberships, 12,183 companies)

| Group | Companies | Description |
|-------|-----------|-------------|
| VEOLIA | 219 | HIDRAQUA, AGBAR, AQUONA, VIAQUA, HIDROGEA, HIDRALIA, SOREA, AQUAMBIENTE, AQUALOGY, Aigües de Barcelona, CASSA AIGÜES |
| FACSA | 67 | Fomento Agrícola Castellonense |
| ELECNOR | 48 | |
| ACCIONA | 48 | ACCIONA Agua |
| SIMETRIA | 47 | Formerly CICLAGUA; includes BECSA |
| SORIGUE | 46 | Includes ACSA Obras e Infraestructuras |
| FCC AQUALIA | 46 | |
| FERROVIAL | 45 | Includes CADAGUA |
| AQLARA | 43 | Includes SOCAMEX, Técnicas Valencianas del Agua (TECVASA) |
| PAVASAL | 42 | Includes PAVAGUA |
| GLOBAL OMNIUM | 36 | Aguas de Valencia, EGEVASA, SEASA, Aigües de Catalunya |
| ACS | 32 | DRAGADOS, DRACE |
| SACYR | 24 | |
| DAM | 22 | Depuración de Aguas del Mediterráneo; includes Agricultores de la Vega |
| TEDAGUA | 16 | |
| SAUR | 14 | Includes GESTAGUA |
| GS INIMA | 12 | |
| INDAQUA | 7 | Includes HIDROGESTIÓN |
| ESPINA Y DELFÍN | 5 | Note: Espina Obras Hidráulicas is NOT part of this group |
| COX | 3 | |

## Ownership Percentage Logic

Companies are assigned to groups with percentage ownership:

- **100%**: Pure company (e.g., "FCC AQUALIA, S.A.")
- **50%**: 2-party JV/UTE (e.g., "UTE FACSA - ACCIONA")
- **33.33%**: 3-party JV (e.g., "UTE FACSA - ISLASFALTO - EXCAVACIONES BUFI")
- **25%**: 4-party JV
- **Special cases**: Some companies have specific percentages (e.g., "Aguas Municipalizadas de Alicante" is 49% VEOLIA)

**Rule for JVs**: If company names are separated by `-`, assume equal split (100% / number of parties).

## Key Views

### 1. `water_wastewater_tenders` (34,140 rows)
Filters all tenders by water/wastewater CPV codes.

**CPV codes included:**
- `651%` - Water distribution
- `655%` - Water-related utility services
- `904%` - Sewerage, wastewater collection/treatment
- `45231%` - Pipelines (water, gas, sewer)
- `45232%` - Water mains, irrigation, sewerage, drainage
- `45240%` - Water projects, hydraulic engineering
- `45247%` - Dams, canals, irrigation channels, aqueducts
- `45248%` - Hydro-mechanical structures
- `45252%` - WTP/WWTP construction
- `45253%` - Chemical treatment plants (desalination)
- `45259%` - Repair/maintenance of treatment plants
- `45262220` - Water-well drilling
- `4416%` - Pipes and fittings
- `44611%` - Tanks and reservoirs
- `44613500` - Water containers (NOT refuse containers)
- `4212%` - Pumps (water, sewage)
- `38421%` - Flowmeters, water meters
- `50514%` - Repair of water treatment equipment
- `7163%` - Water supply monitoring services
- `90713%` - Water/wastewater consultancy
- `90733%` - Water pollution treatment/control
- `90913%` - Tank/reservoir cleaning

**CPV codes excluded:**
- `4523214%` - Heating mains
- `4523222%` - Electricity substations
- `4523223%` - Telecom lines
- `45232470` - Waste transfer stations

### 2. `company_group_view` (822 rows)
Shows company name, group name, and percentage.

```sql
SELECT company_name, group_name, percentage, notes
FROM company_group_view
WHERE group_name = 'VEOLIA';
```

### 3. `unassigned_adjudicatarios` (11,410 rows)
Companies in water tenders not assigned to any group.

### 4. `unassigned_by_tipo_contrato`
Unassigned companies with their tender values by contract type, sorted by Concesión de Servicios DESC.

### 5. `adjudicatarios_by_tipo_contrato`
Pivot table showing all adjudicatarios with values by contract type.

### 6. `company_groups_by_tipo_contrato`
Aggregates weighted values by company group and contract type.

### 7. `company_group_by_year_tipo` (79 rows)
**Main analysis view** - Pivot table showing weighted tender values by:
- Company group
- Contract type (tipo_contrato)
- Year (2017-2025)

**Value calculation:**
```sql
CASE
    WHEN COALESCE(r.valor_estimado_lote, 0) > 0 THEN r.valor_estimado_lote
    ELSE r.presupuesto_base_sin_impuestos_lote
END * (cgm.percentage / 100.0) AS weighted_valor
```

## Common Queries

### Search by company name
```sql
SELECT * FROM water_wastewater_tenders
WHERE adjudicatario ILIKE '%aqualia%';
```

### View company group totals by year
```sql
SELECT group_name, tipo_contrato,
       ROUND(y2024, 2) as "2024",
       ROUND(y2025, 2) as "2025",
       ROUND(total, 2) as "Total"
FROM company_group_by_year_tipo
WHERE group_name = 'FCC AQUALIA'
ORDER BY total DESC;
```

### Find unassigned companies with high concession values
```sql
SELECT * FROM unassigned_by_tipo_contrato
LIMIT 50;
```

### Add a new company to a group
```sql
-- 1. Insert company if not exists
INSERT INTO companies (name) VALUES ('COMPANY NAME')
ON CONFLICT (name) DO NOTHING;

-- 2. Link to group with percentage
INSERT INTO company_group_memberships (company_id, group_id, percentage)
SELECT c.id, g.id, 100.00
FROM companies c, company_groups g
WHERE c.name = 'COMPANY NAME' AND g.name = 'GROUP NAME';
```

### Bulk add companies matching a pattern
```sql
-- Insert all matching companies
INSERT INTO companies (name)
SELECT DISTINCT adjudicatario
FROM water_wastewater_tenders
WHERE adjudicatario ILIKE '%pattern%'
ON CONFLICT (name) DO NOTHING;

-- Link to group (100% for pure entities)
INSERT INTO company_group_memberships (company_id, group_id, percentage)
SELECT c.id, g.id, 100.00
FROM companies c
JOIN company_groups g ON g.name = 'GROUP NAME'
WHERE c.name ILIKE '%pattern%'
  AND c.name NOT ILIKE '%-%'  -- Exclude JVs
  AND NOT EXISTS (
      SELECT 1 FROM company_group_memberships cgm
      WHERE cgm.company_id = c.id AND cgm.group_id = g.id
  );

-- Link JVs with 50% (2-party)
INSERT INTO company_group_memberships (company_id, group_id, percentage)
SELECT c.id, g.id, 50.00
FROM companies c
JOIN company_groups g ON g.name = 'GROUP NAME'
WHERE c.name ILIKE '%pattern%'
  AND c.name ILIKE '%-%'
  AND c.name NOT ILIKE '%-%-%'  -- Exactly one separator
  AND NOT EXISTS (
      SELECT 1 FROM company_group_memberships cgm
      WHERE cgm.company_id = c.id AND cgm.group_id = g.id
  );
```

### Export view to CSV
```sql
COPY (
    SELECT group_name, tipo_contrato,
           ROUND(y2017, 2), ROUND(y2018, 2), ROUND(y2019, 2),
           ROUND(y2020, 2), ROUND(y2021, 2), ROUND(y2022, 2),
           ROUND(y2023, 2), ROUND(y2024, 2), ROUND(y2025, 2),
           ROUND(total, 2)
    FROM company_group_by_year_tipo
    ORDER BY group_name, total DESC
) TO STDOUT WITH CSV HEADER;
```

## Contract Types (tipo_contrato)

| Type | Description |
|------|-------------|
| Obras | Construction works |
| Servicios | Services |
| Suministros | Supplies |
| Concesión de Servicios | Service concessions (long-term) |
| Concesión de Obras | Works concessions |
| Concesión de Obras Públicas | Public works concessions |
| Gestión de Servicios Públicos | Public service management |
| Colaboración entre el sector público y sector privado | PPP |
| Administrativo especial | Special administrative |
| Patrimonial | Property-related |
| Privado | Private |

## Data Statistics

### Records by Year
| Year | Licitaciones | Resultados |
|------|--------------|------------|
| 2017 | 40,660 | 44,772 |
| 2018 | 76,053 | 110,457 |
| 2019 | 111,187 | 162,987 |
| 2020 | 119,896 | 147,682 |
| 2021 | 145,470 | 180,656 |
| 2022 | 169,000 | 215,970 |
| 2023 | 181,011 | 242,466 |
| 2024 | 199,618 | 253,256 |
| 2025 | 204,017 | 253,071 |

## Important Notes

1. **DISTINCT ON pattern**: Always use `DISTINCT ON (l.identificador, r.lote)` with `ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC` to get the most recent version of each tender/lot combination.

2. **JV detection**: Company names with `-` separators indicate joint ventures (UTEs). Count separators to determine number of parties.

3. **Value fallback**: Use `valor_estimado_lote` if > 0, otherwise fall back to `presupuesto_base_sin_impuestos_lote`.

4. **Character encoding issues**: Some company names have corrupted characters (e.g., `AGR�?COLA` instead of `AGRÍCOLA`). Search with patterns to catch these.

5. **Espina Obras Hidráulicas**: This is NOT part of "Espina y Delfín" group - they are different companies.

6. **SIMETRIA**: This group was renamed from CICLAGUA. BECSA companies are part of this group.

7. **Partial ownership**: "Aguas Municipalizadas de Alicante" is 49% VEOLIA (minority stake).

## Importing New Data

```bash
# Import specific years
python3 import_data.py --years "2024,2025" --no-schema

# Import all available years (recreates schema)
python3 import_data.py

# Schema only
python3 import_data.py --schema-only
```

## Future Work

- Continue assigning unassigned adjudicatarios to groups (11,410 remaining)
- Add CPV code descriptions to cpv_codes table
- Create additional analysis views for specific use cases
- Export more detailed reports by region (lugar_ejecucion)
