-- ============================================================================
-- BigQuery Schema: Unified EU public procurement data model
-- ============================================================================
-- Project: eu-spanish-tender-dataset
-- Dataset: EU_Spanish_Tender_Dataset
-- ============================================================================

-- Reference tables
CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_contract_nature` (
    code STRING NOT NULL,
    label STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_procedure_type` (
    code STRING NOT NULL,
    label STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_authority_type` (
    code STRING NOT NULL,
    label STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_notice_type` (
    code STRING NOT NULL,
    label STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_status` (
    code STRING NOT NULL,
    label STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_country` (
    code STRING NOT NULL,
    label STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_currency` (
    code STRING NOT NULL,
    label STRING NOT NULL,
    symbol STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_source` (
    code STRING NOT NULL,
    label STRING NOT NULL,
    url STRING,
    country STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_cpv_codes` (
    code STRING NOT NULL,
    description STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_tags` (
    code STRING NOT NULL,
    label STRING NOT NULL,
    description STRING
);

-- Main contracts table
CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.contracts` (
    source STRING NOT NULL,
    source_id STRING NOT NULL,
    lot_number STRING NOT NULL,
    reference_number STRING,
    source_url STRING,
    status STRING,
    contract_nature STRING,
    is_concession BOOL,
    contract_type_original STRING,
    procedure_type STRING,
    notice_type STRING,
    contract_title STRING,
    lot_title STRING,
    contract_duration STRING,
    contracting_authority STRING,
    authority_id STRING,
    authority_type STRING,
    authority_dir3 STRING,
    place_of_execution STRING,
    nuts_code STRING,
    country STRING,
    estimated_value NUMERIC,
    estimated_value_currency STRING,
    base_budget NUMERIC,
    base_budget_currency STRING,
    awardee STRING,
    awardee_id STRING,
    award_value NUMERIC,
    award_value_with_tax NUMERIC,
    award_value_currency STRING,
    num_offers INT64,
    excluded_low_offers BOOL,
    date_published TIMESTAMP,
    date_updated TIMESTAMP,
    date_awarded DATE,
    date_contract_start DATE,
    eu_funded BOOL,
    is_aggregated BOOL,
    cpv_codes ARRAY<STRING>,
    last_synced_at TIMESTAMP
);

-- Tagging
CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.contracts_tags` (
    source STRING NOT NULL,
    source_id STRING NOT NULL,
    lot_number STRING NOT NULL,
    tag STRING NOT NULL,
    auto_tagged BOOL,
    manually_set BOOL,
    tagged_at TIMESTAMP
);

-- Company tables
CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.companies` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    notes STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.companies_groups` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    description STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.companies_groups_memberships` (
    id INT64 NOT NULL,
    company_id INT64 NOT NULL,
    group_id INT64 NOT NULL,
    percentage NUMERIC,
    notes STRING
);
