-- Water resilience contracts: Early Warning/SAIH, Dam Safety, Stormwater/Flood Risk
-- Combines Spanish (PLACE) and Catalan (Transparencia Catalunya) data
-- Includes Confederaciones Hidrográficas contracts
-- Uses proper joins for CPV codes and tender links
-- Export to CSV: \copy (...) TO 'exports/water_resilience_spain_cat.csv' WITH CSV HEADER

-- SPANISH DATA
SELECT DISTINCT
    'España' as source,
    CASE
        WHEN l.objeto_contrato ILIKE '%SAIH%' OR l.objeto_contrato ILIKE '%información hidrológica%'
             OR l.objeto_contrato ILIKE '%alerta temprana%' OR l.objeto_contrato ILIKE '%predicción de avenidas%'
        THEN 'Early Warning / SAIH'
        WHEN l.objeto_contrato ILIKE '%seguridad de presa%' OR l.objeto_contrato ILIKE '%seguridad%presa%embalse%'
             OR l.objeto_contrato ILIKE '%plan de emergencia%presa%'
             OR l.objeto_contrato ILIKE '%auscultación%presa%' OR l.objeto_contrato ILIKE '%normas de explotación%presa%'
             OR l.objeto_contrato ILIKE '%presa%auscultación%' OR l.objeto_contrato ILIKE '%presa%instrumentación%'
             OR l.objeto_contrato ILIKE '%vigilancia%presa%titularidad%'
        THEN 'Dam Safety'
        ELSE 'Stormwater / Flood Risk'
    END as category,
    l.objeto_contrato as tender_object,
    r.adjudicatario as operator,
    l.year_source as year,
    ROUND(r.importe_adjudicacion_sin_impuestos, 2) as contract_value_eur,
    l.organo_contratacion as contracting_authority,
    (SELECT string_agg(DISTINCT cpv.cpv_code, ', ' ORDER BY cpv.cpv_code)
     FROM licitaciones_cpv cpv WHERE cpv.licitacion_id = l.id) as cpv_codes,
    l.link_licitacion as tender_link
FROM licitaciones l
JOIN resultados r ON l.identificador = r.identificador AND l.year_source = r.year_source
WHERE r.adjudicatario IS NOT NULL
AND r.importe_adjudicacion_sin_impuestos > 10000
AND (
    -- Early warning / SAIH
    (l.objeto_contrato ILIKE '%SAIH%' AND l.objeto_contrato NOT ILIKE '%energía%SAIH%')
    OR l.objeto_contrato ILIKE '%información hidrológica%'
    OR (l.objeto_contrato ILIKE '%alerta temprana%' AND (
        l.objeto_contrato ILIKE '%inundación%' OR l.objeto_contrato ILIKE '%hidrológic%'
        OR l.objeto_contrato ILIKE '%meteorológic%' OR l.objeto_contrato ILIKE '%agua%valladolid%'
    ))
    OR l.objeto_contrato ILIKE '%predicción de avenidas%'
    -- Dam safety
    OR l.objeto_contrato ILIKE '%seguridad de presa%'
    OR l.objeto_contrato ILIKE '%seguridad%presa%embalse%'
    OR l.objeto_contrato ILIKE '%plan de emergencia%presa%'
    OR (l.objeto_contrato ILIKE '%auscultación%' AND l.objeto_contrato ILIKE '%presa%')
    OR l.objeto_contrato ILIKE '%normas de explotación%presa%'
    OR l.objeto_contrato ILIKE '%registro%seguridad%presa%'
    OR (l.objeto_contrato ILIKE '%mejora%seguridad%' AND l.objeto_contrato ILIKE '%presa%')
    OR (l.objeto_contrato ILIKE '%vigilancia%presa%' AND l.objeto_contrato ILIKE '%titularidad%')
    -- Stormwater / flood risk
    OR l.objeto_contrato ILIKE '%aguas pluviales%'
    OR l.objeto_contrato ILIKE '%drenaje urbano%'
    OR l.objeto_contrato ILIKE '%tanque de tormentas%'
    OR l.objeto_contrato ILIKE '%tanques de tormenta%'
    OR l.objeto_contrato ILIKE '%red de pluviales%'
    OR l.objeto_contrato ILIKE '%colector de pluviales%'
    OR (l.objeto_contrato ILIKE '%aliviadero%' AND (l.objeto_contrato ILIKE '%embalse%' OR l.objeto_contrato ILIKE '%presa%'
        OR l.objeto_contrato ILIKE '%tormentas%' OR l.objeto_contrato ILIKE '%estanque%'))
    OR l.objeto_contrato ILIKE '%drenaje sostenible%'
    OR l.objeto_contrato ILIKE '%inundabilidad%'
    OR l.objeto_contrato ILIKE '%gestión de inundaciones%'
    OR (l.objeto_contrato ILIKE '%riesgo de inundación%' AND l.objeto_contrato NOT ILIKE '%seguro%')
    OR l.objeto_contrato ILIKE '%reducción%riesgo%inundación%'
)
-- Exclusions
AND l.objeto_contrato NOT ILIKE '%vigilancia presencial%'
AND l.objeto_contrato NOT ILIKE '%ciberseguridad%'
AND l.objeto_contrato NOT ILIKE '%incendio%forestal%'
AND l.objeto_contrato NOT ILIKE '%influenza%aviar%'
AND l.objeto_contrato NOT ILIKE '%Fuerzas Armadas%'
AND l.objeto_contrato NOT ILIKE '%deporti%'
AND l.objeto_contrato NOT ILIKE '%CCN-CERT%'
AND l.objeto_contrato NOT ILIKE '%medicamentos%'
AND l.objeto_contrato NOT ILIKE '%prórroga%'
AND l.objeto_contrato NOT ILIKE '%seguridad industrial%'
AND l.objeto_contrato NOT ILIKE '%cafetería%'
AND l.objeto_contrato NOT ILIKE '%cafeteria%'
AND l.objeto_contrato NOT ILIKE '%restaurante%'
AND l.objeto_contrato NOT ILIKE '%vending%'
AND l.objeto_contrato NOT ILIKE '%máquinas expendedoras%'
AND l.objeto_contrato NOT ILIKE '%central receptora%alarma%'
AND l.objeto_contrato NOT ILIKE '%sistemas de seguridad%vigilancia%'
-- School/building renovation exclusions
AND l.objeto_contrato NOT ILIKE '%CEIP%'
AND l.objeto_contrato NOT ILIKE '%CEE %'
AND l.objeto_contrato NOT ILIKE '%CEO %'
AND l.objeto_contrato NOT ILIKE '%reforma de vestuarios%'
AND l.objeto_contrato NOT ILIKE '%reforma%baños%CEIP%'

UNION ALL

-- CATALAN DATA (with stricter filters)
SELECT
    'Catalunya' as source,
    CASE
        WHEN objecte_contracte ILIKE '%SAIH%' OR objecte_contracte ILIKE '%informació hidrològica%'
        THEN 'Early Warning / SAIH'
        WHEN (objecte_contracte ILIKE '%seguretat de la presa%'
              OR objecte_contracte ILIKE '%seguretat de presa%'
              OR objecte_contracte ILIKE '%seguretat%preses%embassaments%'
              OR objecte_contracte ILIKE '%normativa de seguretat de preses%')
        THEN 'Dam Safety'
        WHEN (objecte_contracte ILIKE '%auscultació%' AND (objecte_contracte ILIKE '%presa%' OR objecte_contracte ILIKE '%embassament%'))
        THEN 'Dam Safety'
        WHEN objecte_contracte ILIKE '%pla d''emergència%presa%'
        THEN 'Dam Safety'
        WHEN (objecte_contracte ILIKE '%millores%seguretat%' AND objecte_contracte ILIKE '%presa%')
        THEN 'Dam Safety'
        WHEN objecte_contracte ILIKE '%comportes%sobreeixidor%presa%'
        THEN 'Dam Safety'
        ELSE 'Stormwater / Flood Risk'
    END as category,
    COALESCE(objecte_contracte, denominacio) as tender_object,
    denominacio_adjudicatari as operator,
    EXTRACT(YEAR FROM COALESCE(data_adjudicacio_contracte, data_publicacio_contracte))::integer as year,
    ROUND(COALESCE(
        NULLIF(import_adjudicacio_sense, 0),
        NULLIF(valor_estimat_contracte, 0),
        pressupost_base_licitacio
    ), 2) as contract_value_eur,
    nom_organ as contracting_authority,
    codi_cpv as cpv_codes,
    enllac_publicacio as tender_link
FROM catalunya_licitaciones
WHERE fase_publicacio IN ('Adjudicació', 'Formalització')
AND denominacio_adjudicatari IS NOT NULL
AND COALESCE(NULLIF(import_adjudicacio_sense, 0), NULLIF(valor_estimat_contracte, 0), pressupost_base_licitacio) > 10000
AND (
    -- Early warning / SAIH
    objecte_contracte ILIKE '%SAIH%'
    OR objecte_contracte ILIKE '%informació hidrològica%'
    -- Dam safety - STRICT patterns requiring actual dam safety context
    OR objecte_contracte ILIKE '%seguretat de la presa%'
    OR objecte_contracte ILIKE '%seguretat de presa%'
    OR objecte_contracte ILIKE '%seguretat%preses%embassaments%'
    OR objecte_contracte ILIKE '%normativa de seguretat de preses%'
    OR objecte_contracte ILIKE '%pla d''emergència%presa%'
    OR (objecte_contracte ILIKE '%auscultació%' AND (objecte_contracte ILIKE '%presa%' OR objecte_contracte ILIKE '%embassament%'))
    OR (objecte_contracte ILIKE '%millores%seguretat%' AND objecte_contracte ILIKE '%presa%')
    OR objecte_contracte ILIKE '%comportes%sobreeixidor%presa%'
    -- Stormwater / flood risk - focus on contracts where stormwater is the PRIMARY focus
    OR (objecte_contracte ILIKE '%aigües pluvials%' AND objecte_contracte NOT ILIKE '%RSU%' AND objecte_contracte NOT ILIKE '%jardineria%')
    OR objecte_contracte ILIKE '%drenatge urbà%'
    OR objecte_contracte ILIKE '%tanc de tempestes%'
    OR objecte_contracte ILIKE '%xarxa de pluvials%'
    OR objecte_contracte ILIKE '%col·lector de pluvials%'
    OR (objecte_contracte ILIKE '%sobreeixidor%' AND (objecte_contracte ILIKE '%presa%' OR objecte_contracte ILIKE '%embassament%'))
    OR objecte_contracte ILIKE '%inundabilitat%'
    OR objecte_contracte ILIKE '%risc d''inundació%'
    OR objecte_contracte ILIKE '%gestió d''avingudes%'
    OR (objecte_contracte ILIKE '%clavegueram%' AND objecte_contracte ILIKE '%pluvials%'
        AND objecte_contracte NOT ILIKE '%RSU%' AND objecte_contracte NOT ILIKE '%jardineria%')
    OR (objecte_contracte ILIKE '%neteja%' AND objecte_contracte ILIKE '%pluvials%'
        AND objecte_contracte NOT ILIKE '%RSU%' AND objecte_contracte NOT ILIKE '%jardineria%' AND objecte_contracte NOT ILIKE '%viària%')
)
-- Exclusions for mixed municipal services
AND objecte_contracte NOT ILIKE '%RSU%jardineria%'
AND objecte_contracte NOT ILIKE '%Imatge de poble%'
AND objecte_contracte NOT ILIKE '%dipòsit controlat de residus%'
AND objecte_contracte NOT ILIKE '%construcció%vorera%jardineria%'
-- School/building exclusions
AND objecte_contracte NOT ILIKE '%CEIP%'
AND objecte_contracte NOT ILIKE '%reformes%edifici%menjador%'
AND objecte_contracte NOT ILIKE '%remodelació%lavabos%'

ORDER BY category, source, year DESC, contract_value_eur DESC;
