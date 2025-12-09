-- Desalination Plants in Spain - Current Operators
-- Shows the most recent O&M contract for each plant
-- Export to CSV: \copy (...) TO 'exports/desal_plants_operators.csv' WITH CSV HEADER

WITH desal_contracts AS (
    SELECT
        w.lugar_ejecucion as region,
        w.objeto_contrato,
        -- Extract plant name patterns
        CASE
            WHEN w.objeto_contrato ILIKE '%Mutxamel%' OR w.objeto_contrato ILIKE '%Mutxmel%' THEN 'IDAM Mutxamel'
            WHEN w.objeto_contrato ILIKE '%Carboneras%' THEN 'IDAM Carboneras'
            WHEN w.objeto_contrato ILIKE '%Valdelentisco%' THEN 'IDAM Valdelentisco'
            WHEN w.objeto_contrato ILIKE '%Oropesa%' OR w.objeto_contrato ILIKE '%Cabanes%' THEN 'IDAM Cabanes-Oropesa'
            WHEN w.objeto_contrato ILIKE '%Sagunto%' THEN 'IDAM Sagunto'
            WHEN w.objeto_contrato ILIKE '%Moncófar%' OR w.objeto_contrato ILIKE '%Moncofar%' THEN 'IDAM Moncófar'
            WHEN w.objeto_contrato ILIKE '%Alicante I%' AND w.objeto_contrato ILIKE '%Alicante II%' THEN 'IDAM Alicante I & II'
            WHEN w.objeto_contrato ILIKE '%desalinizadora de Alicante%' THEN 'IDAM Alicante'
            WHEN w.objeto_contrato ILIKE '%San Pedro del Pinatar II%' THEN 'IDAM San Pedro del Pinatar II'
            WHEN w.objeto_contrato ILIKE '%San Pedro del Pinatar%' OR w.objeto_contrato ILIKE '%Antonio León%' THEN 'IDAM San Pedro del Pinatar I'
            WHEN w.objeto_contrato ILIKE '%Torrevieja%' THEN 'IDAM Torrevieja'
            WHEN w.objeto_contrato ILIKE '%Águilas%' OR w.objeto_contrato ILIKE '%Aguilas%' THEN 'IDAM Águilas'
            WHEN w.objeto_contrato ILIKE '%Campo de Dalías%' OR w.objeto_contrato ILIKE '%Campo de Dalias%' THEN 'IDAM Campo de Dalías'
            WHEN w.objeto_contrato ILIKE '%Bajo Almanzora%' THEN 'IDAM Bajo Almanzora'
            WHEN w.objeto_contrato ILIKE '%Níjar%' OR w.objeto_contrato ILIKE '%Nijar%' THEN 'IDAM Níjar'
            WHEN w.objeto_contrato ILIKE '%Bahía de Palma%' OR w.objeto_contrato ILIKE '%Bahia de Palma%' THEN 'IDAM Bahía de Palma'
            WHEN (w.objeto_contrato ILIKE '%Santa Eulària%' OR w.objeto_contrato ILIKE '%Santa Eulalia%' OR w.objeto_contrato ILIKE '%Sta. Eulalia%')
                 AND w.objeto_contrato NOT ILIKE '%Sant Antoni%' THEN 'IDAM Santa Eulària (Ibiza)'
            WHEN w.objeto_contrato ILIKE '%Sant Antoni%' THEN 'IDAM Sant Antoni de Portmany (Ibiza)'
            WHEN w.objeto_contrato ILIKE '%planta desaladora%Ibiza%' OR (w.objeto_contrato ILIKE '%Ibiza%' AND w.objeto_contrato ILIKE '%interconexión%') THEN 'IDAM Ibiza'
            WHEN w.objeto_contrato ILIKE '%Formentera%' THEN 'IDAM Formentera'
            WHEN w.objeto_contrato ILIKE '%Ciudadela%' OR w.objeto_contrato ILIKE '%Ciutadella%' THEN 'IDAM Ciutadella (Menorca)'
            WHEN w.objeto_contrato ILIKE '%Andratx%' THEN 'IDAM Andratx (Mallorca)'
            WHEN w.objeto_contrato ILIKE '%Alcudia%' OR w.objeto_contrato ILIKE '%Alcúdia%' THEN 'IDAM Alcúdia (Mallorca)'
            WHEN w.objeto_contrato ILIKE '%Escombreras%' THEN 'IDAM Escombreras'
            WHEN w.objeto_contrato ILIKE '%Mazarrón%' THEN 'IDAM Mazarrón'
            WHEN w.objeto_contrato ILIKE '%Tenerife%' THEN 'IDAM Granadilla (Tenerife)'
            WHEN w.objeto_contrato ILIKE '%Fuerteventura%' THEN 'IDAM Aeropuerto Fuerteventura'
            WHEN w.objeto_contrato ILIKE '%Lanzarote%' THEN 'IDAM Aeropuerto Lanzarote'
            ELSE 'Other'
        END as plant_name,
        CASE
            WHEN w.lugar_ejecucion ILIKE '%Alicante%' THEN 'Alicante'
            WHEN w.lugar_ejecucion ILIKE '%Castellón%' OR w.lugar_ejecucion ILIKE '%Castelló%' THEN 'Castellón'
            WHEN w.lugar_ejecucion ILIKE '%Valencia%' OR w.lugar_ejecucion ILIKE '%València%' THEN 'Valencia'
            WHEN w.lugar_ejecucion ILIKE '%Murcia%' THEN 'Murcia'
            WHEN w.lugar_ejecucion ILIKE '%Almería%' THEN 'Almería'
            WHEN w.lugar_ejecucion ILIKE '%Balears%' OR w.lugar_ejecucion ILIKE '%Mallorca%' OR w.lugar_ejecucion ILIKE '%Eivissa%' THEN 'Illes Balears'
            WHEN w.lugar_ejecucion ILIKE '%Tenerife%' THEN 'Tenerife'
            WHEN w.lugar_ejecucion ILIKE '%Canarias%' THEN 'Canarias'
            WHEN w.lugar_ejecucion ILIKE '%Fuerteventura%' THEN 'Fuerteventura'
            WHEN w.lugar_ejecucion ILIKE '%Lanzarote%' THEN 'Lanzarote'
            WHEN w.lugar_ejecucion ILIKE '%Madrid%' THEN 'ACUAES (national)'
            ELSE w.lugar_ejecucion
        END as province,
        w.adjudicatario as operator,
        COALESCE(g.name, 'Sin asignar') as company_group,
        w.year_source as year,
        ROUND(COALESCE(
            NULLIF(w.importe_adjudicacion_sin_impuestos, 0),
            NULLIF(w.valor_estimado_lote, 0),
            w.presupuesto_base_sin_impuestos_lote
        ), 2) as contract_value_eur,
        ROW_NUMBER() OVER (PARTITION BY
            CASE
                WHEN w.objeto_contrato ILIKE '%Mutxamel%' OR w.objeto_contrato ILIKE '%Mutxmel%' THEN 'IDAM Mutxamel'
                WHEN w.objeto_contrato ILIKE '%Carboneras%' THEN 'IDAM Carboneras'
                WHEN w.objeto_contrato ILIKE '%Valdelentisco%' THEN 'IDAM Valdelentisco'
                WHEN w.objeto_contrato ILIKE '%Oropesa%' OR w.objeto_contrato ILIKE '%Cabanes%' THEN 'IDAM Cabanes-Oropesa'
                WHEN w.objeto_contrato ILIKE '%Sagunto%' THEN 'IDAM Sagunto'
                WHEN w.objeto_contrato ILIKE '%Moncófar%' OR w.objeto_contrato ILIKE '%Moncofar%' THEN 'IDAM Moncófar'
                WHEN w.objeto_contrato ILIKE '%Alicante I%' AND w.objeto_contrato ILIKE '%Alicante II%' THEN 'IDAM Alicante I & II'
                WHEN w.objeto_contrato ILIKE '%desalinizadora de Alicante%' THEN 'IDAM Alicante'
                WHEN w.objeto_contrato ILIKE '%San Pedro del Pinatar II%' THEN 'IDAM San Pedro del Pinatar II'
                WHEN w.objeto_contrato ILIKE '%San Pedro del Pinatar%' OR w.objeto_contrato ILIKE '%Antonio León%' THEN 'IDAM San Pedro del Pinatar I'
                WHEN w.objeto_contrato ILIKE '%Torrevieja%' THEN 'IDAM Torrevieja'
                WHEN w.objeto_contrato ILIKE '%Águilas%' OR w.objeto_contrato ILIKE '%Aguilas%' THEN 'IDAM Águilas'
                WHEN w.objeto_contrato ILIKE '%Campo de Dalías%' OR w.objeto_contrato ILIKE '%Campo de Dalias%' THEN 'IDAM Campo de Dalías'
                WHEN w.objeto_contrato ILIKE '%Bajo Almanzora%' THEN 'IDAM Bajo Almanzora'
                WHEN w.objeto_contrato ILIKE '%Níjar%' OR w.objeto_contrato ILIKE '%Nijar%' THEN 'IDAM Níjar'
                WHEN w.objeto_contrato ILIKE '%Bahía de Palma%' OR w.objeto_contrato ILIKE '%Bahia de Palma%' THEN 'IDAM Bahía de Palma'
                WHEN (w.objeto_contrato ILIKE '%Santa Eulària%' OR w.objeto_contrato ILIKE '%Santa Eulalia%' OR w.objeto_contrato ILIKE '%Sta. Eulalia%')
                     AND w.objeto_contrato NOT ILIKE '%Sant Antoni%' THEN 'IDAM Santa Eulària (Ibiza)'
                WHEN w.objeto_contrato ILIKE '%Sant Antoni%' THEN 'IDAM Sant Antoni de Portmany (Ibiza)'
                WHEN w.objeto_contrato ILIKE '%planta desaladora%Ibiza%' OR (w.objeto_contrato ILIKE '%Ibiza%' AND w.objeto_contrato ILIKE '%interconexión%') THEN 'IDAM Ibiza'
                WHEN w.objeto_contrato ILIKE '%Formentera%' THEN 'IDAM Formentera'
                WHEN w.objeto_contrato ILIKE '%Ciudadela%' OR w.objeto_contrato ILIKE '%Ciutadella%' THEN 'IDAM Ciutadella (Menorca)'
                WHEN w.objeto_contrato ILIKE '%Andratx%' THEN 'IDAM Andratx (Mallorca)'
                WHEN w.objeto_contrato ILIKE '%Alcudia%' OR w.objeto_contrato ILIKE '%Alcúdia%' THEN 'IDAM Alcúdia (Mallorca)'
                WHEN w.objeto_contrato ILIKE '%Escombreras%' THEN 'IDAM Escombreras'
                WHEN w.objeto_contrato ILIKE '%Mazarrón%' THEN 'IDAM Mazarrón'
                WHEN w.objeto_contrato ILIKE '%Tenerife%' THEN 'IDAM Granadilla (Tenerife)'
                WHEN w.objeto_contrato ILIKE '%Fuerteventura%' THEN 'IDAM Aeropuerto Fuerteventura'
                WHEN w.objeto_contrato ILIKE '%Lanzarote%' THEN 'IDAM Aeropuerto Lanzarote'
                ELSE 'Other'
            END
            ORDER BY w.year_source DESC, w.importe_adjudicacion_sin_impuestos DESC NULLS LAST
        ) as rn
    FROM water_wastewater_tenders w
    LEFT JOIN companies c ON w.adjudicatario = c.name
    LEFT JOIN company_group_memberships cgm ON c.id = cgm.company_id
    LEFT JOIN company_groups g ON cgm.group_id = g.id
    WHERE (w.objeto_contrato ILIKE '%desaladora%'
       OR w.objeto_contrato ILIKE '%desalinizadora%'
       OR w.objeto_contrato ILIKE '%desalación%'
       OR w.objeto_contrato ILIKE '%IDAM %'
       OR w.objeto_contrato ILIKE '% IDAM%')
    AND (w.objeto_contrato ILIKE '%operación%'
         OR w.objeto_contrato ILIKE '%mantenimiento%'
         OR w.objeto_contrato ILIKE '%explotación%'
         OR w.objeto_contrato ILIKE '%conservación%'
         OR w.objeto_contrato ILIKE '%gestión%')
    AND w.tipo_contrato IN ('Servicios', 'Concesión de Servicios', 'Gestión de Servicios Públicos')
    AND w.adjudicatario IS NOT NULL
    AND w.adjudicatario != ''
)
SELECT plant_name, province, company_group as current_operator, operator as operator_entity, year as contract_year, contract_value_eur
FROM desal_contracts
WHERE rn = 1 AND plant_name != 'Other'

UNION ALL

-- Catalan plants (operated in-house by ATL)
SELECT 'ITAM Llobregat' as plant_name, 'Catalunya' as province, 'ATL (public)' as current_operator,
       'Aigües Ter Llobregat (ATL)' as operator_entity, 2025 as contract_year, NULL as contract_value_eur
UNION ALL
SELECT 'ITAM Tordera', 'Catalunya', 'ATL (public)', 'Aigües Ter Llobregat (ATL)', 2025, NULL

ORDER BY province, plant_name;
