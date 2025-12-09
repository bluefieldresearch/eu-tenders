-- Water & Wastewater related tenders filtered by CPV codes
-- Modify the adjudicatario filter as needed (use '%%' for all, or '%company%' to filter)

SELECT DISTINCT ON (l.identificador, r.lote)
    l.identificador,
    l.numero_expediente,
    l.fecha_actualizacion,
    l.objeto_contrato,
    l.tipo_contrato,
    l.organo_contratacion,
    l.lugar_ejecucion,
    l.tipo_administracion,
    r.lote,
    r.valor_estimado_lote,
    r.presupuesto_base_sin_impuestos_lote,
    r.adjudicatario,
    r.importe_adjudicacion_sin_impuestos,
    r.num_ofertas,
    r.fecha_entrada_vigor,
    l.year_source,
    (SELECT string_agg(DISTINCT rc2.cpv_code, ', ')
     FROM resultados_cpv rc2
     WHERE rc2.resultado_id = r.id) AS cpv_codes,
    l.link_licitacion
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
    -- Adjudicatario filter: change '%%' to '%company%' or remove line for all
    AND (r.adjudicatario ILIKE '%%' OR r.adjudicatario IS NULL)
ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC;
