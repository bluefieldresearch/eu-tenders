-- WWTP Retrofit/Upgrade Construction Contracts in Spain
-- Filters for: Construction (Obras) contracts related to WWTP upgrades, retrofits, expansions
-- Export to CSV: \copy (...) TO 'exports/wwtp_retrofit_obras.csv' WITH CSV HEADER

SELECT
    w.lugar_ejecucion as provincia,
    w.objeto_contrato as tender_object,
    w.adjudicatario as contractor,
    COALESCE(g.name, 'Sin asignar') as company_group,
    cgm.percentage,
    w.year_source as year,
    ROUND(COALESCE(
        NULLIF(w.importe_adjudicacion_sin_impuestos, 0),
        NULLIF(w.valor_estimado_lote, 0),
        w.presupuesto_base_sin_impuestos_lote
    ), 2) as contract_value_eur,
    w.organo_contratacion as contracting_authority,
    w.identificador,
    w.link_licitacion
FROM water_wastewater_tenders w
LEFT JOIN companies c ON w.adjudicatario = c.name
LEFT JOIN company_group_memberships cgm ON c.id = cgm.company_id
LEFT JOIN company_groups g ON cgm.group_id = g.id
WHERE
    -- Must be Construction contract
    w.tipo_contrato = 'Obras'
    -- Must reference WWTP (EDAR, depuradora, depuración, estación depuradora)
    AND (w.objeto_contrato ILIKE '%EDAR%'
         OR w.objeto_contrato ILIKE '%depuradora%'
         OR w.objeto_contrato ILIKE '%depuración%'
         OR w.objeto_contrato ILIKE '%estación depuradora%'
         OR w.objeto_contrato ILIKE '%tratamiento de aguas residuales%')
    -- Must indicate retrofit/upgrade/improvement/expansion/renovation
    AND (w.objeto_contrato ILIKE '%mejora%'
         OR w.objeto_contrato ILIKE '%ampliación%'
         OR w.objeto_contrato ILIKE '%reforma%'
         OR w.objeto_contrato ILIKE '%renovación%'
         OR w.objeto_contrato ILIKE '%modernización%'
         OR w.objeto_contrato ILIKE '%adecuación%'
         OR w.objeto_contrato ILIKE '%actualización%'
         OR w.objeto_contrato ILIKE '%remodelación%'
         OR w.objeto_contrato ILIKE '%acondicionamiento%'
         OR w.objeto_contrato ILIKE '%rehabilitación%'
         OR w.objeto_contrato ILIKE '%optimización%'
         OR w.objeto_contrato ILIKE '%adaptación%')
    -- Must have an adjudicatario (awarded)
    AND w.adjudicatario IS NOT NULL
    AND w.adjudicatario != ''
    -- Minimum value filter
    AND COALESCE(NULLIF(w.importe_adjudicacion_sin_impuestos, 0), NULLIF(w.valor_estimado_lote, 0), w.presupuesto_base_sin_impuestos_lote) > 50000
    -- Exclude contract modifications and extensions
    AND w.objeto_contrato NOT ILIKE '%prórroga%'
    AND w.objeto_contrato NOT ILIKE '%prorroga%'
    AND w.objeto_contrato NOT ILIKE '%modificado nº%'
    AND w.objeto_contrato NOT ILIKE '%modificación del contrato%'
ORDER BY w.year_source DESC, contract_value_eur DESC NULLS LAST;
