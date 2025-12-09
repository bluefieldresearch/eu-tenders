-- Search tenders by adjudicatario (company name)
-- Modify the adjudicatario filter: change '%company%' to search

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
WHERE r.adjudicatario ILIKE '%pavagua%'  -- Change this to search for any company
ORDER BY l.identificador, r.lote, l.fecha_actualizacion DESC;
