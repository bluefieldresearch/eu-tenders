-- Pivot table: Adjudicatarios by tipo_contrato for Water & Wastewater tenders
-- Shows total awarded value per company broken down by contract type

WITH water_tenders AS (
    SELECT DISTINCT ON (l.identificador, r.lote)
        r.adjudicatario,
        l.tipo_contrato,
        r.importe_adjudicacion_sin_impuestos
    FROM licitaciones l
    JOIN resultados r ON l.identificador = r.identificador
        AND l.year_source = r.year_source
    JOIN resultados_cpv rc ON r.id = rc.resultado_id
    WHERE
        -- CPV filter: Water & Wastewater related codes
        (
            rc.cpv_code LIKE '651%'      -- Water distribution
            OR rc.cpv_code LIKE '655%'   -- Water-related utility services
            OR rc.cpv_code LIKE '904%'   -- Sewerage, wastewater collection/treatment
            OR rc.cpv_code LIKE '45231%' -- Pipelines (water, gas, sewer)
            OR rc.cpv_code LIKE '45232%' -- Water mains, irrigation, sewerage, drainage
            OR rc.cpv_code LIKE '45240%' -- Water projects, hydraulic engineering
            OR rc.cpv_code LIKE '45247%' -- Dams, canals, irrigation channels, aqueducts
            OR rc.cpv_code LIKE '45248%' -- Hydro-mechanical structures
            OR rc.cpv_code LIKE '45252%' -- WTP/WWTP construction
            OR rc.cpv_code LIKE '45253%' -- Chemical treatment plants (desalination)
            OR rc.cpv_code LIKE '45259%' -- Repair/maintenance of treatment plants
            OR rc.cpv_code = '45262220'  -- Water-well drilling
            OR rc.cpv_code LIKE '4416%'  -- Pipes and fittings
            OR rc.cpv_code LIKE '44611%' -- Tanks and reservoirs
            OR rc.cpv_code = '44613500'  -- Water containers only (NOT refuse containers)
            OR rc.cpv_code LIKE '4212%'  -- Pumps (water, sewage)
            OR rc.cpv_code LIKE '38421%' -- Flowmeters, water meters
            OR rc.cpv_code LIKE '50514%' -- Repair of water treatment equipment only (NOT general pumps)
            OR rc.cpv_code LIKE '7163%'  -- Water supply monitoring services
            OR rc.cpv_code LIKE '90713%' -- Water/wastewater consultancy
            OR rc.cpv_code LIKE '90733%' -- Water pollution treatment/control
            OR rc.cpv_code LIKE '90913%' -- Tank/reservoir cleaning
        )
        -- Exclude false positives
        AND rc.cpv_code NOT LIKE '4523214%' -- Heating mains
        AND rc.cpv_code NOT LIKE '4523222%' -- Electricity substations
        AND rc.cpv_code NOT LIKE '4523223%' -- Telecom lines
        AND rc.cpv_code <> '45232470'       -- Waste transfer stations
    ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC
)
SELECT
    adjudicatario,
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Obras'), 0) AS "Obras",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Servicios'), 0) AS "Servicios",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Suministros'), 0) AS "Suministros",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Concesión de Servicios'), 0) AS "Concesion_Servicios",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Concesión de Obras'), 0) AS "Concesion_Obras",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Concesión de Obras Públicas'), 0) AS "Concesion_Obras_Publicas",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Gestión de Servicios Públicos'), 0) AS "Gestion_Servicios_Publicos",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Colaboración entre el sector público y sector privado'), 0) AS "Colaboracion_Publico_Privado",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Administrativo especial'), 0) AS "Administrativo_Especial",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Patrimonial'), 0) AS "Patrimonial",
    COALESCE(SUM(importe_adjudicacion_sin_impuestos) FILTER (WHERE tipo_contrato = 'Privado'), 0) AS "Privado",
    SUM(importe_adjudicacion_sin_impuestos) AS "Total"
FROM water_tenders
WHERE adjudicatario IS NOT NULL
GROUP BY adjudicatario
ORDER BY "Total" DESC NULLS LAST;
