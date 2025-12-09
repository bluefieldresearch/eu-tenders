-- Pipe Replacement Tenders in Water & Wastewater Sector
-- Analysis of network renovation/replacement contracts in Spain
--
-- IMPORTANT FINDING: Catalunya shows very few pipe replacement contracts in public
-- procurement (only 3 identified 2017-2025). This is because water services in
-- Catalunya are predominantly managed by private operators (AGBAR/Veolia, etc.)
-- whose internal renewal works don't appear in public procurement databases.
--
-- Created: 2025-12-03

-- Drop existing view if needed
DROP VIEW IF EXISTS pipe_replacement_tenders;

-- Create view for pipe replacement tenders
CREATE VIEW pipe_replacement_tenders AS
SELECT
    identificador,
    numero_expediente,
    fecha_actualizacion,
    objeto_contrato,
    tipo_contrato,
    organo_contratacion,
    lugar_ejecucion,
    tipo_administracion,
    lote,
    valor_estimado_lote,
    presupuesto_base_sin_impuestos_lote,
    adjudicatario,
    importe_adjudicacion_sin_impuestos,
    num_ofertas,
    fecha_entrada_vigor,
    year_source,
    cpv_codes,
    link_licitacion,
    CASE
        WHEN lugar_ejecucion LIKE 'ES51%'
          OR lugar_ejecucion ILIKE '%cataluña%'
          OR lugar_ejecucion ILIKE '%catalunya%'
          OR lugar_ejecucion ILIKE '%(barcelona)%'
          OR lugar_ejecucion ILIKE '%(tarragona)%'
          OR lugar_ejecucion ILIKE '%(lleida)%'
          OR lugar_ejecucion ILIKE '%(girona)%'
        THEN 'Catalunya'
        ELSE 'Rest of Spain'
    END as region
FROM water_wastewater_tenders
WHERE (
    -- Spanish: Renovation/replacement of water/sewer networks
    objeto_contrato ~* 'renovaci[oó]n.*(tuber[ií]a|red|conducci[oó]n).*(agua|abastecimiento|saneamiento|alcantarillado|potable)'
    OR objeto_contrato ~* 'sustituci[oó]n.*(tuber[ií]a|red|conducci[oó]n).*(agua|abastecimiento|saneamiento|alcantarillado|potable)'
    OR objeto_contrato ~* 'reposici[oó]n.*(tuber[ií]a|red|conducci[oó]n).*(agua|abastecimiento|saneamiento|alcantarillado|potable)'
    OR objeto_contrato ~* '(agua|abastecimiento|saneamiento|alcantarillado|potable).*(renovaci[oó]n|sustituci[oó]n|reposici[oó]n).*(tuber[ií]a|red|conducci[oó]n)'
    -- Fibrocemento (asbestos-cement) pipe replacement
    OR objeto_contrato ~* 'sustituci[oó]n.*(fibrocemento|amianto|fibrociment)'
    OR objeto_contrato ~* 'renovaci[oó]n.*(fibrocemento|amianto|fibrociment)'
    -- Additional Spanish patterns
    OR objeto_contrato ~* 'reforma.*(red|tuber[ií]a).*(agua|abastecimiento|saneamiento)'
    OR objeto_contrato ~* 'mejora.*(red|tuber[ií]a).*(agua|abastecimiento|saneamiento)'
    -- Catalan: xarxa (network), clavegueram (sewerage), canonada (pipe)
    OR objeto_contrato ~* 'renovaci[oó].*(xarxa|canonada).*(aigua|abastament|clavegueram|sanejament)'
    OR objeto_contrato ~* 'substituci[oó].*(xarxa|canonada).*(aigua|abastament|clavegueram|sanejament)'
    OR objeto_contrato ~* 'reposici[oó].*(xarxa|canonada).*(aigua|abastament|clavegueram|sanejament)'
    OR objeto_contrato ~* '(aigua|abastament|clavegueram|sanejament).*(renovaci[oó]|substituci[oó]|reposici[oó]).*(xarxa|canonada)'
    -- Modernization/improvement of distribution networks (water/sewer)
    OR objeto_contrato ~* 'millora.*(xarxa|canonada).*(aigua|clavegueram)'
    OR objeto_contrato ~* 'modernitz.*(xarxa|canonada).*(aigua|clavegueram)'
);

-- Summary by region and year
SELECT
    region,
    year_source,
    COUNT(*) as num_contratos,
    ROUND(SUM(COALESCE(NULLIF(valor_estimado_lote, 0), presupuesto_base_sin_impuestos_lote)) / 1000000, 2) as valor_M_EUR,
    ROUND(SUM(importe_adjudicacion_sin_impuestos) / 1000000, 2) as adjudicado_M_EUR
FROM pipe_replacement_tenders
GROUP BY region, year_source
ORDER BY region, year_source;

-- Overall summary by region
SELECT
    region,
    COUNT(*) as num_contratos,
    ROUND(SUM(COALESCE(NULLIF(valor_estimado_lote, 0), presupuesto_base_sin_impuestos_lote)) / 1000000, 2) as valor_M_EUR,
    ROUND(SUM(importe_adjudicacion_sin_impuestos) / 1000000, 2) as adjudicado_M_EUR,
    ROUND(AVG(COALESCE(NULLIF(valor_estimado_lote, 0), presupuesto_base_sin_impuestos_lote)), 0) as valor_medio_EUR
FROM pipe_replacement_tenders
GROUP BY region
ORDER BY region;

-- Top adjudicatarios by weighted value (using company_group_memberships if available)
SELECT
    adjudicatario,
    COUNT(*) as num_contratos,
    ROUND(SUM(COALESCE(NULLIF(valor_estimado_lote, 0), presupuesto_base_sin_impuestos_lote)) / 1000000, 2) as valor_M_EUR
FROM pipe_replacement_tenders
GROUP BY adjudicatario
ORDER BY valor_M_EUR DESC
LIMIT 30;

-- By tipo_contrato
SELECT
    tipo_contrato,
    COUNT(*) as num_contratos,
    ROUND(SUM(COALESCE(NULLIF(valor_estimado_lote, 0), presupuesto_base_sin_impuestos_lote)) / 1000000, 2) as valor_M_EUR,
    ROUND(SUM(importe_adjudicacion_sin_impuestos) / 1000000, 2) as adjudicado_M_EUR
FROM pipe_replacement_tenders
GROUP BY tipo_contrato
ORDER BY valor_M_EUR DESC;
