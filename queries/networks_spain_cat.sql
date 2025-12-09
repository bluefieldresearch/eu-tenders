-- Water/wastewater network O&M contracts in Spain + Catalonia with company groups
-- Combines Spanish (PLACE) and Catalan (Transparencia Catalunya) data
-- Export to CSV: \copy (...) TO 'exports/networks_spain_cat.csv' WITH CSV HEADER

-- Spanish Networks
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
AND w.objeto_contrato NOT ILIKE '%EDAR%'
AND w.objeto_contrato NOT ILIKE '%depuradora%'
AND w.objeto_contrato NOT ILIKE '%desaladora%'
AND w.objeto_contrato NOT ILIKE '%desalinizadora%'
AND w.objeto_contrato NOT ILIKE '%IDAM%'
AND w.objeto_contrato NOT ILIKE '%basura%'
AND w.objeto_contrato NOT ILIKE '%limpieza viaria%'
AND w.objeto_contrato NOT ILIKE '%ingeniería%'

UNION ALL

-- Catalan Networks
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
AND (objecte_contracte ILIKE '%abastament%'
   OR objecte_contracte ILIKE '%abastiment%'
   OR objecte_contracte ILIKE '%sanejament%'
   OR objecte_contracte ILIKE '%xarxa d''aigua%'
   OR objecte_contracte ILIKE '%clavegueram%'
   OR objecte_contracte ILIKE '%cicle de l''aigua%'
   OR objecte_contracte ILIKE '%cicle integral%'
   OR objecte_contracte ILIKE '%subministrament d''aigua%'
   OR objecte_contracte ILIKE '%servei d''aigua%'
   OR objecte_contracte ILIKE '%gestió de l''aigua%')
AND (objecte_contracte ILIKE '%explotació%'
     OR objecte_contracte ILIKE '%manteniment%'
     OR objecte_contracte ILIKE '%operació%'
     OR objecte_contracte ILIKE '%gestió%'
     OR tipus_contracte = 'Serveis')
AND denominacio_adjudicatari IS NOT NULL
AND denominacio_adjudicatari != ''
AND COALESCE(NULLIF(import_adjudicacio_sense, 0), NULLIF(valor_estimat_contracte, 0), pressupost_base_licitacio) > 50000
AND objecte_contracte NOT ILIKE '%pròrroga%'
AND objecte_contracte NOT ILIKE '%modificat%'
AND objecte_contracte NOT ILIKE '%EDAR%'
AND objecte_contracte NOT ILIKE '%depuradora%'
AND objecte_contracte NOT ILIKE '%dessalinitzadora%'
AND objecte_contracte NOT ILIKE '%enginyeria%'

ORDER BY source, year DESC, contract_value_eur DESC;
