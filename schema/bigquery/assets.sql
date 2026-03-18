-- ============================================================================
-- Asset & Locality Schema (BigQuery)
-- ============================================================================

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.ref_asset_types` (
    code STRING NOT NULL,
    label STRING NOT NULL,
    description STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.localities` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    country STRING NOT NULL,
    nuts_code STRING,
    municipality_code STRING,
    population INT64
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.assets` (
    id INT64 NOT NULL,
    locality_id INT64 NOT NULL,
    asset_type STRING NOT NULL,
    name STRING,
    capacity STRING,
    year_built INT64,
    latitude NUMERIC,
    longitude NUMERIC,
    notes STRING
);

CREATE TABLE IF NOT EXISTS `EU_Spanish_Tender_Dataset.assets_operators` (
    id INT64 NOT NULL,
    asset_id INT64 NOT NULL,
    operator STRING,
    start_date DATE,
    end_date DATE,
    notes STRING
);
