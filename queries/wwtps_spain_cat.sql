-- Water/wastewater WWTP O&M contracts in Spain + Catalonia with company groups
-- Combines Spanish (PLACE) and Catalan (Transparencia Catalunya) data
-- Export to CSV: \copy (...) TO 'exports/wwtps_spain_cat.csv' WITH CSV HEADER

-- Spanish WWTPs
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
WHERE (w.objeto_contrato ILIKE '%EDAR%'
   OR w.objeto_contrato ILIKE '%depuradora%'
   OR w.objeto_contrato ILIKE '%estación depuradora%'
   OR w.objeto_contrato ILIKE '%depuración de aguas%'
   OR w.objeto_contrato ILIKE '%tratamiento de aguas residuales%')
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
AND w.objeto_contrato NOT ILIKE '%ingeniería%'

UNION ALL

-- Catalan WWTPs
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
AND (objecte_contracte ILIKE '%EDAR%'
   OR objecte_contracte ILIKE '%depuradora%'
   OR objecte_contracte ILIKE '%depuració%'
   OR objecte_contracte ILIKE '%tractament d''aigües residuals%')
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
AND objecte_contracte NOT ILIKE '%enginyeria%'

ORDER BY source, year DESC, contract_value_eur DESC;
