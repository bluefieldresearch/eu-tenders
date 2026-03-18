-- ============================================================================
-- Schema v2: Unified EU public procurement data model
-- ============================================================================
-- Designed to consolidate multiple procurement data sources (PLACE, GENCAT,
-- TED, etc.) into a single structure with EU-standard taxonomies.
--
-- Old tables (licitaciones, resultados, etc.) are NOT dropped here.
-- They are preserved so migrate_v2.sql can read from them.
-- Run cleanup_v1.sql after verifying the migration.
-- ============================================================================

-- ============================================================================
-- SECTION 1: Reference Tables
-- ============================================================================

DROP TABLE IF EXISTS contracts CASCADE;
DROP TABLE IF EXISTS ref_contract_nature CASCADE;
DROP TABLE IF EXISTS ref_procedure_type CASCADE;
DROP TABLE IF EXISTS ref_authority_type CASCADE;
DROP TABLE IF EXISTS ref_notice_type CASCADE;
DROP TABLE IF EXISTS ref_status CASCADE;
DROP TABLE IF EXISTS ref_country CASCADE;
DROP TABLE IF EXISTS ref_currency CASCADE;
DROP TABLE IF EXISTS ref_source CASCADE;
DROP TABLE IF EXISTS ref_cpv_codes CASCADE;

-- Contract nature (EU eForms: contract-nature)
CREATE TABLE ref_contract_nature (
    code VARCHAR(20) PRIMARY KEY,
    label VARCHAR(100) NOT NULL
);

INSERT INTO ref_contract_nature (code, label) VALUES
    ('works', 'Works'),
    ('services', 'Services'),
    ('supplies', 'Supplies');

-- Procedure type (EU eForms: procurement-procedure-type + extensions)
CREATE TABLE ref_procedure_type (
    code VARCHAR(30) PRIMARY KEY,
    label VARCHAR(200) NOT NULL
);

INSERT INTO ref_procedure_type (code, label) VALUES
    ('open', 'Open'),
    ('restricted', 'Restricted'),
    ('neg-w-call', 'Negotiated with prior publication of a call for competition'),
    ('neg-wo-call', 'Negotiated without prior call for competition'),
    ('comp-dial', 'Competitive dialogue'),
    ('innovation', 'Innovation partnership'),
    ('comp-tend', 'Competitive tendering'),
    ('oth-single', 'Other single stage procedure'),
    ('oth-mult', 'Other multiple stage procedure'),
    ('minor', 'Minor contract'),
    ('internal', 'Internal procedure'),
    ('design-contest', 'Design contest'),
    ('dynamic-acq', 'Dynamic acquisition system');

-- Authority type (EU eForms: buyer-legal-type + extensions)
CREATE TABLE ref_authority_type (
    code VARCHAR(30) PRIMARY KEY,
    label VARCHAR(200) NOT NULL
);

INSERT INTO ref_authority_type (code, label) VALUES
    ('cga', 'Central government authority'),
    ('ra', 'Regional authority'),
    ('la', 'Local authority'),
    ('body-pl', 'Body governed by public law'),
    ('body-pl-cga', 'Body governed by public law, controlled by a central government authority'),
    ('body-pl-la', 'Body governed by public law, controlled by a local authority'),
    ('body-pl-ra', 'Body governed by public law, controlled by a regional authority'),
    ('pub-undert', 'Public undertaking'),
    ('pub-undert-cga', 'Public undertaking, controlled by a central government authority'),
    ('pub-undert-la', 'Public undertaking, controlled by a local authority'),
    ('pub-undert-ra', 'Public undertaking, controlled by a regional authority'),
    ('org-sub', 'Organisation awarding a contract subsidised by a contracting authority'),
    ('eu-ins-bod-ag', 'EU institution, body or agency'),
    ('int-org', 'International organisation'),
    ('def-cont', 'Defence contractor'),
    ('spec-rights-entity', 'Entity with special or exclusive rights'),
    ('university', 'University'),
    ('other', 'Other');

-- Notice type (EU eForms: notice-type + extensions)
CREATE TABLE ref_notice_type (
    code VARCHAR(30) PRIMARY KEY,
    label VARCHAR(200) NOT NULL
);

INSERT INTO ref_notice_type (code, label) VALUES
    ('cn-standard', 'Contract or concession notice — standard regime'),
    ('cn-social', 'Contract notice — light regime'),
    ('cn-desg', 'Design contest notice'),
    ('can-standard', 'Contract or concession award notice — standard regime'),
    ('can-social', 'Contract or concession award notice — light regime'),
    ('can-modif', 'Contract modification notice'),
    ('can-desg', 'Design contest result notice'),
    ('can-tran', 'Contract award notice for public passenger transport services'),
    ('pin-only', 'Prior information notice used only for information'),
    ('pin-cfc-standard', 'Prior information notice used as call for competition — standard regime'),
    ('pin-cfc-social', 'Prior information notice used as call for competition — light regime'),
    ('pin-buyer', 'Notice of publication of a prior information notice on a buyer profile'),
    ('pin-rtl', 'Prior information notice used to shorten time limits'),
    ('pin-tran', 'Prior information notice for public passenger transport services'),
    ('compl', 'Contract completion notice'),
    ('veat', 'Voluntary ex-ante transparency notice'),
    ('subco', 'Subcontracting notice'),
    ('pmc', 'Pre-market consultation notice'),
    ('qu-sy', 'Notice on the existence of a qualification system'),
    ('aggregated', 'Aggregated minor contracts publication');

-- Contract status (custom taxonomy)
CREATE TABLE ref_status (
    code VARCHAR(20) PRIMARY KEY,
    label VARCHAR(100) NOT NULL
);

INSERT INTO ref_status (code, label) VALUES
    ('announced', 'Announced'),
    ('evaluation', 'Under evaluation'),
    ('awarded', 'Awarded'),
    ('formalized', 'Formalized'),
    ('cancelled', 'Cancelled'),
    ('archived', 'Archived'),
    ('modified', 'Modified'),
    ('prior-notice', 'Prior information notice'),
    ('deserted', 'Deserted');

-- Country (ISO 3166-1 alpha-2)
CREATE TABLE ref_country (
    code VARCHAR(5) PRIMARY KEY,
    label VARCHAR(100) NOT NULL
);

INSERT INTO ref_country (code, label) VALUES
    ('ES', 'Spain'),
    ('FR', 'France'),
    ('PT', 'Portugal'),
    ('IT', 'Italy'),
    ('DE', 'Germany'),
    ('SE', 'Sweden'),
    ('DK', 'Denmark'),
    ('NL', 'Netherlands'),
    ('BE', 'Belgium'),
    ('AT', 'Austria'),
    ('PL', 'Poland'),
    ('RO', 'Romania'),
    ('GR', 'Greece'),
    ('CZ', 'Czech Republic'),
    ('IE', 'Ireland'),
    ('FI', 'Finland'),
    ('HR', 'Croatia'),
    ('BG', 'Bulgaria'),
    ('SK', 'Slovakia'),
    ('LT', 'Lithuania'),
    ('SI', 'Slovenia'),
    ('LV', 'Latvia'),
    ('EE', 'Estonia'),
    ('CY', 'Cyprus'),
    ('LU', 'Luxembourg'),
    ('MT', 'Malta');

-- Currency (ISO 4217)
CREATE TABLE ref_currency (
    code VARCHAR(5) PRIMARY KEY,
    label VARCHAR(100) NOT NULL,
    symbol VARCHAR(5)
);

INSERT INTO ref_currency (code, label, symbol) VALUES
    ('EUR', 'Euro', '€'),
    ('SEK', 'Swedish krona', 'kr'),
    ('DKK', 'Danish krone', 'kr'),
    ('PLN', 'Polish zloty', 'zł'),
    ('CZK', 'Czech koruna', 'Kč'),
    ('HUF', 'Hungarian forint', 'Ft'),
    ('RON', 'Romanian leu', 'lei'),
    ('BGN', 'Bulgarian lev', 'лв'),
    ('HRK', 'Croatian kuna', 'kn'),
    ('GBP', 'British pound', '£'),
    ('NOK', 'Norwegian krone', 'kr'),
    ('CHF', 'Swiss franc', 'CHF');

-- Data source
CREATE TABLE ref_source (
    code VARCHAR(20) PRIMARY KEY,
    label VARCHAR(200) NOT NULL,
    url TEXT,
    country VARCHAR(5) REFERENCES ref_country(code)
);

INSERT INTO ref_source (code, label, url, country) VALUES
    ('PLACE', 'Plataforma de Contratación del Estado', 'https://contrataciondelestado.es', 'ES'),
    ('GENCAT', 'Plataforma de Contractació Pública de Catalunya', 'https://contractaciopublica.cat', 'ES'),
    ('TED', 'Tenders Electronic Daily', 'https://ted.europa.eu', NULL);

-- CPV codes (Common Procurement Vocabulary)
CREATE TABLE ref_cpv_codes (
    code VARCHAR(10) PRIMARY KEY,
    description TEXT
);

-- Populate from existing cpv_codes table if it exists
INSERT INTO ref_cpv_codes (code, description)
SELECT code, description FROM cpv_codes
ON CONFLICT (code) DO NOTHING;


-- ============================================================================
-- SECTION 2: Contracts Table
-- ============================================================================

CREATE TABLE contracts (
    -- Identity (composite primary key)
    source VARCHAR(20) NOT NULL REFERENCES ref_source(code),
    source_id VARCHAR(200) NOT NULL,
    lot_number VARCHAR(200) NOT NULL DEFAULT '0',

    -- Reference
    reference_number VARCHAR(200),
    source_url TEXT,

    -- Classification
    status VARCHAR(20) REFERENCES ref_status(code),
    contract_nature VARCHAR(20) REFERENCES ref_contract_nature(code),
    is_concession BOOLEAN DEFAULT FALSE,
    contract_type_original VARCHAR(200),
    procedure_type VARCHAR(30) REFERENCES ref_procedure_type(code),
    notice_type VARCHAR(30) REFERENCES ref_notice_type(code),

    -- Contract details
    contract_title TEXT,
    lot_title TEXT,
    contract_duration VARCHAR(200),

    -- Contracting authority
    contracting_authority TEXT,
    authority_id VARCHAR(50),
    authority_type VARCHAR(30) REFERENCES ref_authority_type(code),
    authority_dir3 VARCHAR(50),

    -- Location
    place_of_execution VARCHAR(500),
    nuts_code VARCHAR(20),
    country VARCHAR(5) REFERENCES ref_country(code),

    -- Values
    estimated_value DECIMAL(18, 2),
    estimated_value_currency VARCHAR(5) DEFAULT 'EUR' REFERENCES ref_currency(code),
    base_budget DECIMAL(18, 2),
    base_budget_currency VARCHAR(5) DEFAULT 'EUR' REFERENCES ref_currency(code),

    -- Award
    awardee TEXT,
    awardee_id TEXT,
    award_value DECIMAL(18, 2),
    award_value_with_tax DECIMAL(18, 2),
    award_value_currency VARCHAR(5) DEFAULT 'EUR' REFERENCES ref_currency(code),
    num_offers INTEGER,
    excluded_low_offers BOOLEAN,

    -- Dates
    date_published TIMESTAMP,
    date_updated TIMESTAMP,
    date_awarded DATE,
    date_contract_start DATE,

    -- Extra
    eu_funded BOOLEAN,
    is_aggregated BOOLEAN,
    cpv_codes TEXT[],

    -- Sync metadata
    last_synced_at TIMESTAMP DEFAULT NOW(),

    -- Primary key
    PRIMARY KEY (source, source_id, lot_number)
);


-- ============================================================================
-- SECTION 3: Indexes
-- ============================================================================

CREATE INDEX idx_contracts_source ON contracts(source);
CREATE INDEX idx_contracts_status ON contracts(status);
CREATE INDEX idx_contracts_nature ON contracts(contract_nature);
CREATE INDEX idx_contracts_concession ON contracts(is_concession);
CREATE INDEX idx_contracts_procedure ON contracts(procedure_type);
CREATE INDEX idx_contracts_authority ON contracts(contracting_authority);
CREATE INDEX idx_contracts_authority_type ON contracts(authority_type);
CREATE INDEX idx_contracts_country ON contracts(country);
CREATE INDEX idx_contracts_place ON contracts(place_of_execution);
CREATE INDEX idx_contracts_awardee ON contracts(awardee);
CREATE INDEX idx_contracts_date_published ON contracts(date_published);
CREATE INDEX idx_contracts_date_updated ON contracts(date_updated);
CREATE INDEX idx_contracts_date_awarded ON contracts(date_awarded);
CREATE INDEX idx_contracts_reference ON contracts(reference_number);
CREATE INDEX idx_contracts_cpv ON contracts USING gin(cpv_codes);

-- Full text search (using 'simple' config for multi-language support)
CREATE INDEX idx_contracts_title_fts ON contracts USING gin(to_tsvector('simple', contract_title));
CREATE INDEX idx_contracts_awardee_fts ON contracts USING gin(to_tsvector('simple', awardee));


-- ============================================================================
-- SECTION 4: Company Tables
-- ============================================================================

CREATE TABLE companies_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE,
    notes TEXT
);

CREATE TABLE companies_groups_memberships (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES companies_groups(id) ON DELETE CASCADE,
    percentage DECIMAL(5, 2) DEFAULT 100.00,
    notes TEXT
);

CREATE INDEX idx_companies_name ON companies(name);
CREATE INDEX idx_companies_groups_memberships_company ON companies_groups_memberships(company_id);
CREATE INDEX idx_companies_groups_memberships_group ON companies_groups_memberships(group_id);


-- ============================================================================
-- SECTION 5: Tagging System
-- ============================================================================
-- Flexible classification layer for contracts. Supports multiple independent
-- tag categories (sector, subsector, theme, cohort, etc.) with both automatic
-- and manual tagging. Manual overrides are preserved across auto-tag runs.

DROP TABLE IF EXISTS contracts_tags CASCADE;
DROP TABLE IF EXISTS ref_tags CASCADE;

CREATE TABLE ref_tags (
    code VARCHAR(100) PRIMARY KEY,
    label VARCHAR(200) NOT NULL,
    description TEXT
);

INSERT INTO ref_tags (code, label, description) VALUES
    ('water', 'Water & Wastewater', 'Water supply, wastewater treatment, drainage, irrigation'),
    ('water-supply', 'Water Supply', 'Drinking water production and distribution'),
    ('wastewater', 'Wastewater', 'Wastewater collection and treatment'),
    ('desalination', 'Desalination', 'Seawater and brackish water desalination');

CREATE TABLE contracts_tags (
    source VARCHAR(20) NOT NULL,
    source_id VARCHAR(200) NOT NULL,
    lot_number VARCHAR(200) NOT NULL,
    tag VARCHAR(100) NOT NULL REFERENCES ref_tags(code),
    auto_tagged BOOLEAN DEFAULT FALSE,
    manually_set BOOLEAN DEFAULT FALSE,
    tagged_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (source, source_id, lot_number, tag),
    FOREIGN KEY (source, source_id, lot_number) REFERENCES contracts(source, source_id, lot_number) ON DELETE CASCADE
);

CREATE INDEX idx_contracts_tags_tag ON contracts_tags(tag);
CREATE INDEX idx_contracts_tags_auto ON contracts_tags(auto_tagged);
CREATE INDEX idx_contracts_tags_manual ON contracts_tags(manually_set);
