-- Desalination O&M contracts in Spain with company groups
-- Export to CSV: \copy (...) TO 'exports/desalination_spain.csv' WITH CSV HEADER

SELECT
    w.lugar_ejecucion as provincia,
    w.objeto_contrato as tender_object,
    w.adjudicatario as operator,
    COALESCE(g.name, 'Sin asignar') as company_group,
    cgm.percentage,
    w.year_source as year,
    ROUND(COALESCE(
        NULLIF(w.importe_adjudicacion_sin_impuestos, 0),
        NULLIF(w.valor_estimado_lote, 0),
        w.presupuesto_base_sin_impuestos_lote
    ), 2) as contract_value_eur,
    w.organo_contratacion as contracting_authority
FROM water_wastewater_tenders w
LEFT JOIN companies c ON w.adjudicatario = c.name
LEFT JOIN company_group_memberships cgm ON c.id = cgm.company_id
LEFT JOIN company_groups g ON cgm.group_id = g.id
WHERE (w.objeto_contrato ILIKE '%desaladora%'
   OR w.objeto_contrato ILIKE '%desalinizadora%'
   OR w.objeto_contrato ILIKE '%desalación%'
   OR w.objeto_contrato ILIKE '%dessaladora%'
   OR w.objeto_contrato ILIKE '%IDAM %'
   OR w.objeto_contrato ILIKE '% IDAM%'
   OR w.objeto_contrato ILIKE '%EDAM%')
AND (w.objeto_contrato ILIKE '%operación%'
     OR w.objeto_contrato ILIKE '%mantenimiento%'
     OR w.objeto_contrato ILIKE '%explotación%'
     OR w.objeto_contrato ILIKE '%conservación%'
     OR w.objeto_contrato ILIKE '%gestión%'
     OR w.objeto_contrato ILIKE '%funcionamiento%')
AND w.tipo_contrato IN ('Servicios', 'Concesión de Servicios', 'Gestión de Servicios Públicos')
AND w.adjudicatario IS NOT NULL
AND w.adjudicatario != ''
AND COALESCE(NULLIF(w.importe_adjudicacion_sin_impuestos, 0), NULLIF(w.valor_estimado_lote, 0), w.presupuesto_base_sin_impuestos_lote) > 50000
AND w.objeto_contrato NOT ILIKE '%prórroga%'
AND w.objeto_contrato NOT ILIKE '%prorroga%'
AND w.objeto_contrato NOT ILIKE '%modificado%'
AND w.objeto_contrato NOT ILIKE '%modificación%'
AND w.objeto_contrato NOT ILIKE '%ingeniería%'
ORDER BY w.lugar_ejecucion, w.year_source DESC, w.importe_adjudicacion_sin_impuestos DESC;
