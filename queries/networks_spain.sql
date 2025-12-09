-- Water/wastewater network O&M contracts in Spain with company groups
-- Includes: abastecimiento, saneamiento, redes, alcantarillado, ciclo integral
-- Excludes: WWTPs (EDAR), desalination (IDAM), extensions (prórroga/modificación)
-- Export to CSV: \copy (...) TO 'exports/networks_spain.csv' WITH CSV HEADER

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
WHERE (w.objeto_contrato ILIKE '%abastecimiento%'
   OR w.objeto_contrato ILIKE '%saneamiento%'
   OR w.objeto_contrato ILIKE '%red de agua%'
   OR w.objeto_contrato ILIKE '%redes de agua%'
   OR w.objeto_contrato ILIKE '%alcantarillado%'
   OR w.objeto_contrato ILIKE '%ciclo integral%'
   OR w.objeto_contrato ILIKE '%suministro de agua%'
   OR w.objeto_contrato ILIKE '%servicio de agua%'
   OR w.objeto_contrato ILIKE '%gestión del agua%')
AND (w.objeto_contrato ILIKE '%operación%'
     OR w.objeto_contrato ILIKE '%mantenimiento%'
     OR w.objeto_contrato ILIKE '%explotación%'
     OR w.objeto_contrato ILIKE '%conservación%'
     OR w.objeto_contrato ILIKE '%gestión%')
AND w.tipo_contrato IN ('Servicios', 'Concesión de Servicios', 'Gestión de Servicios Públicos')
AND w.adjudicatario IS NOT NULL
AND w.adjudicatario != ''
AND COALESCE(NULLIF(w.importe_adjudicacion_sin_impuestos, 0), NULLIF(w.valor_estimado_lote, 0), w.presupuesto_base_sin_impuestos_lote) > 50000
AND w.objeto_contrato NOT ILIKE '%prórroga%'
AND w.objeto_contrato NOT ILIKE '%prorroga%'
AND w.objeto_contrato NOT ILIKE '%modificado%'
AND w.objeto_contrato NOT ILIKE '%modificación%'
-- Exclude WWTPs and desalination (separate exports)
AND w.objeto_contrato NOT ILIKE '%EDAR%'
AND w.objeto_contrato NOT ILIKE '%depuradora%'
AND w.objeto_contrato NOT ILIKE '%desaladora%'
AND w.objeto_contrato NOT ILIKE '%desalinizadora%'
AND w.objeto_contrato NOT ILIKE '%IDAM%'
AND w.objeto_contrato NOT ILIKE '%basura%'
AND w.objeto_contrato NOT ILIKE '%limpieza viaria%'
AND w.objeto_contrato NOT ILIKE '%ingeniería%'
ORDER BY w.lugar_ejecucion, w.year_source DESC, w.importe_adjudicacion_sin_impuestos DESC;
