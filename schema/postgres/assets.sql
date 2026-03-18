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
    locality_id INTEGER NOT NULL REFERENCES localities(id),
    asset_type VARCHAR(50) NOT NULL REFERENCES ref_asset_types(code),
    name VARCHAR(500),
    capacity VARCHAR(200),
    year_built INTEGER,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    notes TEXT
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
    start_date DATE,
    end_date DATE,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_assets_operators_asset ON assets_operators(asset_id);
CREATE INDEX IF NOT EXISTS idx_assets_operators_operator ON assets_operators(operator);
