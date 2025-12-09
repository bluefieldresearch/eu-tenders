-- PostgreSQL schema for Spanish public tender data
-- Water & Wastewater sector analysis

-- ============================================================================
-- SECTION 1: Core Tables (from government data import)
-- ============================================================================

-- Drop existing tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS resultados_cpv CASCADE;
DROP TABLE IF EXISTS licitaciones_cpv CASCADE;
DROP TABLE IF EXISTS resultados CASCADE;
DROP TABLE IF EXISTS licitaciones CASCADE;
DROP TABLE IF EXISTS cpv_codes CASCADE;
DROP TABLE IF EXISTS company_group_memberships CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS company_groups CASCADE;

-- CPV codes reference table (Common Procurement Vocabulary)
CREATE TABLE cpv_codes (
    code VARCHAR(10) PRIMARY KEY,
    description TEXT
);

-- Main tenders table (Licitaciones)
CREATE TABLE licitaciones (
    id SERIAL PRIMARY KEY,
    identificador BIGINT NOT NULL,
    link_licitacion TEXT,
    fecha_actualizacion TIMESTAMP,
    estado VARCHAR(50),  -- Vigente/Anulada/Archivada
    numero_expediente VARCHAR(100),
    objeto_contrato TEXT,
    valor_estimado DECIMAL(18, 2),
    presupuesto_base_sin_impuestos DECIMAL(18, 2),
    tipo_contrato VARCHAR(100),
    lugar_ejecucion VARCHAR(200),
    organo_contratacion TEXT,
    nif_oc VARCHAR(20),
    dir3 VARCHAR(50),
    tipo_administracion TEXT,
    year_source INTEGER NOT NULL,  -- Which year file this came from
    UNIQUE(identificador, year_source)
);

-- Results table (Resultados) - can have multiple results per tender (lots)
CREATE TABLE resultados (
    id SERIAL PRIMARY KEY,
    identificador BIGINT NOT NULL,
    link_licitacion TEXT,
    fecha_actualizacion TIMESTAMP,
    numero_expediente VARCHAR(100),
    lote VARCHAR(200),
    objeto_lote TEXT,
    valor_estimado_lote DECIMAL(18, 2),
    presupuesto_base_sin_impuestos_lote DECIMAL(18, 2),
    num_ofertas INTEGER,
    ofertas_excluidas_bajas BOOLEAN,
    fecha_entrada_vigor DATE,
    adjudicatario TEXT,
    importe_adjudicacion_sin_impuestos DECIMAL(18, 2),
    year_source INTEGER NOT NULL  -- Which year file this came from
);

-- Junction table for CPV codes in licitaciones (many-to-many)
CREATE TABLE licitaciones_cpv (
    id SERIAL PRIMARY KEY,
    licitacion_id INTEGER NOT NULL REFERENCES licitaciones(id) ON DELETE CASCADE,
    cpv_code VARCHAR(10) NOT NULL,
    position INTEGER NOT NULL DEFAULT 1  -- Order of CPV code (1 = primary)
);

-- Junction table for CPV codes in resultados (many-to-many)
CREATE TABLE resultados_cpv (
    id SERIAL PRIMARY KEY,
    resultado_id INTEGER NOT NULL REFERENCES resultados(id) ON DELETE CASCADE,
    cpv_code VARCHAR(10) NOT NULL,
    position INTEGER NOT NULL DEFAULT 1  -- Order of CPV code (1 = primary)
);

-- ============================================================================
-- SECTION 2: Company Group Tables (for market analysis)
-- ============================================================================

-- Company groups (parent organizations / holding companies)
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

-- Membership linking companies to groups with ownership percentage
-- Percentage allows tracking JVs/UTEs where multiple groups participate
CREATE TABLE company_group_memberships (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES company_groups(id) ON DELETE CASCADE,
    percentage DECIMAL(5,2) DEFAULT 100.00,  -- 100% = pure entity, 50% = 2-party JV, etc.
    notes TEXT
);

-- ============================================================================
-- SECTION 3: Indexes for Performance
-- ============================================================================

CREATE INDEX idx_licitaciones_identificador ON licitaciones(identificador);
CREATE INDEX idx_licitaciones_fecha ON licitaciones(fecha_actualizacion);
CREATE INDEX idx_licitaciones_estado ON licitaciones(estado);
CREATE INDEX idx_licitaciones_tipo_contrato ON licitaciones(tipo_contrato);
CREATE INDEX idx_licitaciones_organo ON licitaciones(organo_contratacion);
CREATE INDEX idx_licitaciones_nif ON licitaciones(nif_oc);
CREATE INDEX idx_licitaciones_year ON licitaciones(year_source);

CREATE INDEX idx_resultados_identificador ON resultados(identificador);
CREATE INDEX idx_resultados_fecha ON resultados(fecha_actualizacion);
CREATE INDEX idx_resultados_adjudicatario ON resultados(adjudicatario);
CREATE INDEX idx_resultados_year ON resultados(year_source);

CREATE INDEX idx_licitaciones_cpv_licitacion ON licitaciones_cpv(licitacion_id);
CREATE INDEX idx_licitaciones_cpv_code ON licitaciones_cpv(cpv_code);
CREATE INDEX idx_resultados_cpv_resultado ON resultados_cpv(resultado_id);
CREATE INDEX idx_resultados_cpv_code ON resultados_cpv(cpv_code);

CREATE INDEX idx_companies_name ON companies(name);
CREATE INDEX idx_company_group_memberships_company ON company_group_memberships(company_id);
CREATE INDEX idx_company_group_memberships_group ON company_group_memberships(group_id);

-- Full text search indexes
CREATE INDEX idx_licitaciones_objeto_fts ON licitaciones USING gin(to_tsvector('spanish', objeto_contrato));
CREATE INDEX idx_resultados_objeto_fts ON resultados USING gin(to_tsvector('spanish', objeto_lote));
CREATE INDEX idx_resultados_adjudicatario_fts ON resultados USING gin(to_tsvector('spanish', adjudicatario));

-- ============================================================================
-- SECTION 4: Basic Views (general purpose)
-- ============================================================================

-- Licitaciones with CPV codes as concatenated string and array
CREATE OR REPLACE VIEW licitaciones_con_cpv AS
SELECT
    l.*,
    string_agg(lc.cpv_code, ';' ORDER BY lc.position) as cpv_codes,
    array_agg(lc.cpv_code ORDER BY lc.position) as cpv_array
FROM licitaciones l
LEFT JOIN licitaciones_cpv lc ON l.id = lc.licitacion_id
GROUP BY l.id;

-- Resultados with CPV codes as concatenated string and array
CREATE OR REPLACE VIEW resultados_con_cpv AS
SELECT
    r.*,
    string_agg(rc.cpv_code, ';' ORDER BY rc.position) as cpv_codes,
    array_agg(rc.cpv_code ORDER BY rc.position) as cpv_array
FROM resultados r
LEFT JOIN resultados_cpv rc ON r.id = rc.resultado_id
GROUP BY r.id;

-- Combined view of tenders with their results
CREATE OR REPLACE VIEW licitaciones_resultados AS
SELECT
    l.identificador,
    l.numero_expediente,
    l.objeto_contrato,
    l.valor_estimado,
    l.tipo_contrato,
    l.organo_contratacion,
    l.estado,
    l.year_source,
    r.lote,
    r.adjudicatario,
    r.importe_adjudicacion_sin_impuestos,
    r.num_ofertas,
    r.fecha_entrada_vigor
FROM licitaciones l
LEFT JOIN resultados r ON l.identificador = r.identificador
    AND l.year_source = r.year_source;

-- ============================================================================
-- SECTION 5: Water & Wastewater Sector Views
-- ============================================================================

-- Water & Wastewater tenders filtered by CPV codes
-- CPV codes included:
--   651%      - Water distribution
--   655%      - Water-related utility services
--   904%      - Sewerage, wastewater collection/treatment
--   45231%    - Pipelines (water, gas, sewer)
--   45232%    - Water mains, irrigation, sewerage, drainage
--   45240%    - Water projects, hydraulic engineering
--   45247%    - Dams, canals, irrigation channels, aqueducts
--   45248%    - Hydro-mechanical structures
--   45252%    - WTP/WWTP construction
--   45253%    - Chemical treatment plants (desalination)
--   45259%    - Repair/maintenance of treatment plants
--   45262220  - Water-well drilling
--   4416%     - Pipes and fittings
--   44611%    - Tanks and reservoirs
--   44613500  - Water containers (NOT refuse containers)
--   4212%     - Pumps (water, sewage)
--   38421%    - Flowmeters, water meters
--   50514%    - Repair of water treatment equipment
--   7163%     - Water supply monitoring services
--   90713%    - Water/wastewater consultancy
--   90733%    - Water pollution treatment/control
--   90913%    - Tank/reservoir cleaning
-- CPV codes excluded:
--   4523214%  - Heating mains
--   4523222%  - Electricity substations
--   4523223%  - Telecom lines
--   45232470  - Waste transfer stations

CREATE OR REPLACE VIEW water_wastewater_tenders AS
SELECT DISTINCT ON (l.identificador, r.lote)
    l.identificador,
    l.numero_expediente,
    l.fecha_actualizacion,
    l.objeto_contrato,
    l.tipo_contrato,
    l.organo_contratacion,
    l.lugar_ejecucion,
    l.tipo_administracion,
    r.lote,
    r.valor_estimado_lote,
    r.presupuesto_base_sin_impuestos_lote,
    r.adjudicatario,
    r.importe_adjudicacion_sin_impuestos,
    r.num_ofertas,
    r.fecha_entrada_vigor,
    l.year_source,
    (SELECT string_agg(DISTINCT rc2.cpv_code, ', ')
     FROM resultados_cpv rc2
     WHERE rc2.resultado_id = r.id) AS cpv_codes,
    l.link_licitacion
FROM licitaciones l
JOIN resultados r ON l.identificador = r.identificador
    AND l.year_source = r.year_source
JOIN resultados_cpv rc ON r.id = rc.resultado_id
WHERE
    -- CPV filter: Water & Wastewater related codes
    (
        rc.cpv_code LIKE '651%'
        OR rc.cpv_code LIKE '655%'
        OR rc.cpv_code LIKE '904%'
        OR rc.cpv_code LIKE '45231%'
        OR rc.cpv_code LIKE '45232%'
        OR rc.cpv_code LIKE '45240%'
        OR rc.cpv_code LIKE '45247%'
        OR rc.cpv_code LIKE '45248%'
        OR rc.cpv_code LIKE '45252%'
        OR rc.cpv_code LIKE '45253%'
        OR rc.cpv_code LIKE '45259%'
        OR rc.cpv_code = '45262220'
        OR rc.cpv_code LIKE '4416%'
        OR rc.cpv_code LIKE '44611%'
        OR rc.cpv_code = '44613500'
        OR rc.cpv_code LIKE '4212%'
        OR rc.cpv_code LIKE '38421%'
        OR rc.cpv_code LIKE '50514%'
        OR rc.cpv_code LIKE '7163%'
        OR rc.cpv_code LIKE '90713%'
        OR rc.cpv_code LIKE '90733%'
        OR rc.cpv_code LIKE '90913%'
    )
    -- Exclude false positives
    AND rc.cpv_code NOT LIKE '4523214%'
    AND rc.cpv_code NOT LIKE '4523222%'
    AND rc.cpv_code NOT LIKE '4523223%'
    AND rc.cpv_code <> '45232470'
ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC;

-- ============================================================================
-- SECTION 6: Company Group Views
-- ============================================================================

-- View showing company name, group name, and ownership percentage
CREATE OR REPLACE VIEW company_group_view AS
SELECT
    c.name AS company_name,
    g.name AS group_name,
    cgm.percentage,
    cgm.notes
FROM companies c
JOIN company_group_memberships cgm ON c.id = cgm.company_id
JOIN company_groups g ON cgm.group_id = g.id
ORDER BY g.name, cgm.percentage DESC, c.name;

-- Adjudicatarios in water tenders not assigned to any company group
CREATE OR REPLACE VIEW unassigned_adjudicatarios AS
SELECT DISTINCT w.adjudicatario
FROM water_wastewater_tenders w
LEFT JOIN companies c ON w.adjudicatario = c.name
LEFT JOIN company_group_memberships cgm ON c.id = cgm.company_id
WHERE w.adjudicatario IS NOT NULL
  AND cgm.id IS NULL
ORDER BY w.adjudicatario;

-- Pivot table: All adjudicatarios by contract type (importe_adjudicacion values)
CREATE OR REPLACE VIEW adjudicatarios_by_tipo_contrato AS
WITH water_tenders AS (
    SELECT DISTINCT ON (l.identificador, r.lote)
        r.adjudicatario,
        l.tipo_contrato,
        r.importe_adjudicacion_sin_impuestos
    FROM licitaciones l
    JOIN resultados r ON l.identificador = r.identificador
        AND l.year_source = r.year_source
    JOIN resultados_cpv rc ON r.id = rc.resultado_id
    WHERE
        (
            rc.cpv_code LIKE '651%'
            OR rc.cpv_code LIKE '655%'
            OR rc.cpv_code LIKE '904%'
            OR rc.cpv_code LIKE '45231%'
            OR rc.cpv_code LIKE '45232%'
            OR rc.cpv_code LIKE '45240%'
            OR rc.cpv_code LIKE '45247%'
            OR rc.cpv_code LIKE '45248%'
            OR rc.cpv_code LIKE '45252%'
            OR rc.cpv_code LIKE '45253%'
            OR rc.cpv_code LIKE '45259%'
            OR rc.cpv_code = '45262220'
            OR rc.cpv_code LIKE '4416%'
            OR rc.cpv_code LIKE '44611%'
            OR rc.cpv_code = '44613500'
            OR rc.cpv_code LIKE '4212%'
            OR rc.cpv_code LIKE '38421%'
            OR rc.cpv_code LIKE '50514%'
            OR rc.cpv_code LIKE '7163%'
            OR rc.cpv_code LIKE '90713%'
            OR rc.cpv_code LIKE '90733%'
            OR rc.cpv_code LIKE '90913%'
        )
        AND rc.cpv_code NOT LIKE '4523214%'
        AND rc.cpv_code NOT LIKE '4523222%'
        AND rc.cpv_code NOT LIKE '4523223%'
        AND rc.cpv_code <> '45232470'
    ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC
)
SELECT
    adjudicatario,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Obras'), 0) AS obras,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Servicios'), 0) AS servicios,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Suministros'), 0) AS suministros,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Concesión de Servicios'), 0) AS concesion_servicios,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Concesión de Obras'), 0) AS concesion_obras,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Concesión de Obras Públicas'), 0) AS concesion_obras_publicas,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Gestión de Servicios Públicos'), 0) AS gestion_servicios_publicos,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Colaboración entre el sector público y sector privado'), 0) AS colaboracion_publico_privado,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Administrativo especial'), 0) AS administrativo_especial,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Patrimonial'), 0) AS patrimonial,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Privado'), 0) AS privado,
    SUM(importe_adjudicacion_sin_impuestos) AS total
FROM water_tenders
WHERE adjudicatario IS NOT NULL
GROUP BY adjudicatario;

-- Unassigned adjudicatarios with their values by contract type, sorted by concesion_servicios
CREATE OR REPLACE VIEW unassigned_by_tipo_contrato AS
SELECT
    a.adjudicatario,
    a.obras,
    a.servicios,
    a.suministros,
    a.concesion_servicios,
    a.concesion_obras,
    a.concesion_obras_publicas,
    a.gestion_servicios_publicos,
    a.colaboracion_publico_privado,
    a.administrativo_especial,
    a.patrimonial,
    a.privado,
    a.total
FROM adjudicatarios_by_tipo_contrato a
JOIN unassigned_adjudicatarios u ON a.adjudicatario = u.adjudicatario
ORDER BY a.concesion_servicios DESC NULLS LAST;

-- Company groups aggregated by contract type (weighted by ownership percentage)
-- Uses valor_estimado_lote with fallback to presupuesto_base_sin_impuestos_lote
CREATE OR REPLACE VIEW company_groups_by_tipo_contrato AS
WITH water_tenders AS (
    SELECT DISTINCT ON (l.identificador, r.lote)
        r.adjudicatario,
        l.tipo_contrato,
        CASE
            WHEN COALESCE(r.valor_estimado_lote, 0) > 0 THEN r.valor_estimado_lote
            ELSE r.presupuesto_base_sin_impuestos_lote
        END AS valor
    FROM licitaciones l
    JOIN resultados r ON l.identificador = r.identificador
        AND l.year_source = r.year_source
    JOIN resultados_cpv rc ON r.id = rc.resultado_id
    WHERE
        (
            rc.cpv_code LIKE '651%'
            OR rc.cpv_code LIKE '655%'
            OR rc.cpv_code LIKE '904%'
            OR rc.cpv_code LIKE '45231%'
            OR rc.cpv_code LIKE '45232%'
            OR rc.cpv_code LIKE '45240%'
            OR rc.cpv_code LIKE '45247%'
            OR rc.cpv_code LIKE '45248%'
            OR rc.cpv_code LIKE '45252%'
            OR rc.cpv_code LIKE '45253%'
            OR rc.cpv_code LIKE '45259%'
            OR rc.cpv_code = '45262220'
            OR rc.cpv_code LIKE '4416%'
            OR rc.cpv_code LIKE '44611%'
            OR rc.cpv_code = '44613500'
            OR rc.cpv_code LIKE '4212%'
            OR rc.cpv_code LIKE '38421%'
            OR rc.cpv_code LIKE '50514%'
            OR rc.cpv_code LIKE '7163%'
            OR rc.cpv_code LIKE '90713%'
            OR rc.cpv_code LIKE '90733%'
            OR rc.cpv_code LIKE '90913%'
        )
        AND rc.cpv_code NOT LIKE '4523214%'
        AND rc.cpv_code NOT LIKE '4523222%'
        AND rc.cpv_code NOT LIKE '4523223%'
        AND rc.cpv_code <> '45232470'
    ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC
),
tenders_with_groups AS (
    SELECT
        g.name AS group_name,
        wt.tipo_contrato,
        wt.valor * (cgm.percentage / 100.0) AS weighted_valor
    FROM water_tenders wt
    JOIN companies c ON wt.adjudicatario = c.name
    JOIN company_group_memberships cgm ON c.id = cgm.company_id
    JOIN company_groups g ON cgm.group_id = g.id
    WHERE wt.adjudicatario IS NOT NULL
)
SELECT
    group_name,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Obras'), 0) AS obras,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Servicios'), 0) AS servicios,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Suministros'), 0) AS suministros,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Concesión de Servicios'), 0) AS concesion_servicios,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Concesión de Obras'), 0) AS concesion_obras,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Concesión de Obras Públicas'), 0) AS concesion_obras_publicas,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Gestión de Servicios Públicos'), 0) AS gestion_servicios_publicos,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Colaboración entre el sector público y sector privado'), 0) AS colaboracion_publico_privado,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Administrativo especial'), 0) AS administrativo_especial,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Patrimonial'), 0) AS patrimonial,
    COALESCE(SUM(weighted_valor) FILTER (WHERE tipo_contrato = 'Privado'), 0) AS privado,
    SUM(weighted_valor) AS total
FROM tenders_with_groups
GROUP BY group_name
ORDER BY total DESC NULLS LAST;

-- Company groups by year and contract type (pivot table)
-- Main analysis view for market share over time
-- Uses CROSS JOIN to ensure every group has a row for every contract type (even if 0)
CREATE OR REPLACE VIEW company_group_by_year_tipo AS
WITH contract_types AS (
    SELECT DISTINCT tipo_contrato
    FROM licitaciones
    WHERE tipo_contrato IS NOT NULL
),
water_tenders AS (
    SELECT DISTINCT ON (l.identificador, r.lote)
        r.adjudicatario,
        l.tipo_contrato,
        l.year_source,
        CASE
            WHEN COALESCE(r.valor_estimado_lote, 0) > 0 THEN r.valor_estimado_lote
            ELSE r.presupuesto_base_sin_impuestos_lote
        END AS valor
    FROM licitaciones l
    JOIN resultados r ON l.identificador = r.identificador
        AND l.year_source = r.year_source
    JOIN resultados_cpv rc ON r.id = rc.resultado_id
    WHERE
        (
            rc.cpv_code LIKE '651%'
            OR rc.cpv_code LIKE '655%'
            OR rc.cpv_code LIKE '904%'
            OR rc.cpv_code LIKE '45231%'
            OR rc.cpv_code LIKE '45232%'
            OR rc.cpv_code LIKE '45240%'
            OR rc.cpv_code LIKE '45247%'
            OR rc.cpv_code LIKE '45248%'
            OR rc.cpv_code LIKE '45252%'
            OR rc.cpv_code LIKE '45253%'
            OR rc.cpv_code LIKE '45259%'
            OR rc.cpv_code = '45262220'
            OR rc.cpv_code LIKE '4416%'
            OR rc.cpv_code LIKE '44611%'
            OR rc.cpv_code = '44613500'
            OR rc.cpv_code LIKE '4212%'
            OR rc.cpv_code LIKE '38421%'
            OR rc.cpv_code LIKE '50514%'
            OR rc.cpv_code LIKE '7163%'
            OR rc.cpv_code LIKE '90713%'
            OR rc.cpv_code LIKE '90733%'
            OR rc.cpv_code LIKE '90913%'
        )
        AND rc.cpv_code NOT LIKE '4523214%'
        AND rc.cpv_code NOT LIKE '4523222%'
        AND rc.cpv_code NOT LIKE '4523223%'
        AND rc.cpv_code <> '45232470'
    ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC
),
tenders_with_groups AS (
    SELECT
        g.name AS group_name,
        wt.tipo_contrato,
        wt.year_source,
        wt.valor * (cgm.percentage / 100.0) AS weighted_valor
    FROM water_tenders wt
    JOIN companies c ON wt.adjudicatario = c.name
    JOIN company_group_memberships cgm ON c.id = cgm.company_id
    JOIN company_groups g ON cgm.group_id = g.id
    WHERE wt.adjudicatario IS NOT NULL
),
all_combinations AS (
    SELECT g.name AS group_name, ct.tipo_contrato
    FROM company_groups g
    CROSS JOIN contract_types ct
)
SELECT
    ac.group_name,
    ac.tipo_contrato,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2017), 0) AS y2017,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2018), 0) AS y2018,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2019), 0) AS y2019,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2020), 0) AS y2020,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2021), 0) AS y2021,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2022), 0) AS y2022,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2023), 0) AS y2023,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2024), 0) AS y2024,
    COALESCE(SUM(twg.weighted_valor) FILTER (WHERE twg.year_source = 2025), 0) AS y2025,
    COALESCE(SUM(twg.weighted_valor), 0) AS total
FROM all_combinations ac
LEFT JOIN tenders_with_groups twg
    ON ac.group_name = twg.group_name
    AND ac.tipo_contrato = twg.tipo_contrato
GROUP BY ac.group_name, ac.tipo_contrato
ORDER BY ac.group_name, ac.tipo_contrato;
