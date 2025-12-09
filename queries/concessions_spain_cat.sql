-- Water/wastewater concession contracts in Spain + Catalonia with company groups
-- Combines Spanish (PLACE) and Catalan (Transparencia Catalunya) data
-- Export to CSV: \copy (...) TO 'exports/concessions_spain_cat.csv' WITH CSV HEADER

-- Spanish Concessions
SELECT
    'España' as source,
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
    w.tipo_contrato,
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

UNION ALL

-- Catalan Concessions
SELECT
    'Catalunya' as source,
    lloc_execucio as provincia,
    COALESCE(objecte_contracte, denominacio) as tender_object,
    denominacio_adjudicatari as operator,
    COALESCE(g.name, 'Sin asignar') as company_group,
    cgm.percentage,
    EXTRACT(YEAR FROM COALESCE(data_adjudicacio_contracte, data_publicacio_contracte))::integer as year,
    ROUND(COALESCE(
        NULLIF(import_adjudicacio_sense, 0),
        NULLIF(valor_estimat_contracte, 0),
        pressupost_base_licitacio
    ), 2) as contract_value_eur,
    tipus_contracte as tipo_contrato,
    nom_organ as contracting_authority
FROM catalunya_licitaciones cl
LEFT JOIN companies c ON cl.denominacio_adjudicatari = c.name
LEFT JOIN company_group_memberships cgm ON c.id = cgm.company_id
LEFT JOIN company_groups g ON cgm.group_id = g.id
WHERE fase_publicacio IN ('Adjudicació', 'Formalització')
AND tipus_contracte = 'Concessió de serveis'
AND (objecte_contracte ILIKE '%abastament%'
   OR objecte_contracte ILIKE '%abastiment%'
   OR objecte_contracte ILIKE '%sanejament%'
   OR objecte_contracte ILIKE '%aigua potable%'
   OR objecte_contracte ILIKE '%clavegueram%'
   OR objecte_contracte ILIKE '%cicle de l''aigua%'
   OR objecte_contracte ILIKE '%cicle integral%'
   OR objecte_contracte ILIKE '%subministrament d''aigua%'
   OR objecte_contracte ILIKE '%servei d''aigua%'
   OR objecte_contracte ILIKE '%gestió de l''aigua%'
   OR objecte_contracte ILIKE '%depuració%'
   OR objecte_contracte ILIKE '%EDAR%'
   OR objecte_contracte ILIKE '%depuradora%')
AND denominacio_adjudicatari IS NOT NULL
AND denominacio_adjudicatari != ''
AND COALESCE(NULLIF(import_adjudicacio_sense, 0), NULLIF(valor_estimat_contracte, 0), pressupost_base_licitacio) > 50000
AND objecte_contracte NOT ILIKE '%pròrroga%'
AND objecte_contracte NOT ILIKE '%modificat%'
AND objecte_contracte NOT ILIKE '%enginyeria%'

ORDER BY source, year DESC, contract_value_eur DESC;
