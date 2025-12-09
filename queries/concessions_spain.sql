-- Water/wastewater concession contracts in Spain with company groups
-- Tipo: Concesión de Servicios
-- Export to CSV: \copy (...) TO 'exports/concessions_spain.csv' WITH CSV HEADER

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
WHERE w.tipo_contrato = 'Concesión de Servicios'
AND (w.objeto_contrato ILIKE '%abastecimiento%'
   OR w.objeto_contrato ILIKE '%saneamiento%'
   OR w.objeto_contrato ILIKE '%agua potable%'
   OR w.objeto_contrato ILIKE '%alcantarillado%'
   OR w.objeto_contrato ILIKE '%ciclo integral%'
   OR w.objeto_contrato ILIKE '%suministro de agua%'
   OR w.objeto_contrato ILIKE '%servicio de agua%'
   OR w.objeto_contrato ILIKE '%gestión del agua%'
   OR w.objeto_contrato ILIKE '%servicio integral del agua%'
   OR w.objeto_contrato ILIKE '%depuración%'
   OR w.objeto_contrato ILIKE '%EDAR%'
   OR w.objeto_contrato ILIKE '%depuradora%')
AND w.adjudicatario IS NOT NULL
AND w.adjudicatario != ''
AND COALESCE(NULLIF(w.importe_adjudicacion_sin_impuestos, 0), NULLIF(w.valor_estimado_lote, 0), w.presupuesto_base_sin_impuestos_lote) > 50000
AND w.objeto_contrato NOT ILIKE '%prórroga%'
AND w.objeto_contrato NOT ILIKE '%prorroga%'
AND w.objeto_contrato NOT ILIKE '%modificado%'
AND w.objeto_contrato NOT ILIKE '%modificación%'
AND w.objeto_contrato NOT ILIKE '%ingeniería%'
ORDER BY w.lugar_ejecucion, w.year_source DESC, w.importe_adjudicacion_sin_impuestos DESC;
