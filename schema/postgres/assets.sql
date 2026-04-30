-- ============================================================================
-- Asset & Locality Schema
-- ============================================================================
-- Independent registry of water infrastructure assets and localities.
-- Operator information is stored directly, not linked to contracts.
-- Every locality has at least a water_network and sewer_network asset.
-- ============================================================================

-- ============================================================================
-- Reference Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS ref_asset_types (
    code VARCHAR(50) PRIMARY KEY,
    label VARCHAR(200) NOT NULL,
    description TEXT
);

INSERT INTO ref_asset_types (code, label, description) VALUES
    ('wwtp', 'Wastewater Treatment Plant', 'EDAR, STEP, station d''epuration'),
    ('dwtp', 'Drinking Water Treatment Plant', 'ETAP, potabilizadora, usine de production d''eau potable'),
    ('desalination', 'Desalination Plant', 'Desaladora, dessalinitzadora, usine de dessalement'),
    ('water_network', 'Water Distribution Network', 'Red de abastecimiento, reseau d''eau potable'),
    ('sewer_network', 'Sewer/Wastewater Collection Network', 'Red de saneamiento, reseau d''assainissement')
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- Localities
-- ============================================================================

CREATE TABLE IF NOT EXISTS localities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    country VARCHAR(5) NOT NULL REFERENCES ref_country(code),
    nuts_code VARCHAR(20),
    municipality_code VARCHAR(20),
    population INTEGER,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    UNIQUE(name, country, municipality_code)
);

CREATE INDEX IF NOT EXISTS idx_localities_country ON localities(country);
CREATE INDEX IF NOT EXISTS idx_localities_nuts ON localities(nuts_code);
CREATE INDEX IF NOT EXISTS idx_localities_name ON localities(name);


-- ============================================================================
-- Assets
-- ============================================================================

CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    locality_id INTEGER REFERENCES localities(id),
    asset_type VARCHAR(50) NOT NULL REFERENCES ref_asset_types(code),
    name VARCHAR(500),
    owner VARCHAR(500),
    owner_link TEXT,
    ca VARCHAR(500),
    ca_link TEXT,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    notes TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_assets_locality ON assets(locality_id);
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_assets_name ON assets(name);


-- ============================================================================
-- Operators
-- ============================================================================

CREATE TABLE IF NOT EXISTS assets_operators (
    id SERIAL PRIMARY KEY,
    asset_id INTEGER NOT NULL REFERENCES assets(id),
    operator VARCHAR(500),
    management_type VARCHAR(20),
    contract_type VARCHAR(100),
    start_date DATE,
    end_date DATE,
    tender_link TEXT,
    notes TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_assets_operators_asset ON assets_operators(asset_id);
CREATE INDEX IF NOT EXISTS idx_assets_operators_asset ON assets_operators(asset_id);
CREATE INDEX IF NOT EXISTS idx_assets_operators_operator ON assets_operators(operator);


-- ============================================================================
-- Views
-- ============================================================================

CREATE OR REPLACE VIEW v_water_supply AS
SELECT
    l.country,
    l.name AS municipality,
    l.municipality_code,
    l.population,
    a.asset_type,
    a.owner,
    a.ca,
    ao.operator,
    ao.management_type,
    ao.contract_type,
    ao.start_date,
    ao.end_date,
    ao.tender_link,
    ao.notes,
    a.metadata
FROM assets a
JOIN assets_operators ao ON ao.asset_id = a.id
LEFT JOIN localities l ON a.locality_id = l.id
WHERE a.asset_type = 'water_network';
