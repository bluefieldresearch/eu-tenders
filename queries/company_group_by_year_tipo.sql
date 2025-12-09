-- Company group by year and tipo_contrato
-- Shows weighted values (valor_estimado_lote or presupuesto_base as fallback)
-- multiplied by ownership percentage
-- Change 'FCC AQUALIA' to any group name

SELECT tipo_contrato,
       ROUND(y2017/1000000, 2) as "2017",
       ROUND(y2018/1000000, 2) as "2018",
       ROUND(y2019/1000000, 2) as "2019",
       ROUND(y2020/1000000, 2) as "2020",
       ROUND(y2021/1000000, 2) as "2021",
       ROUND(y2022/1000000, 2) as "2022",
       ROUND(y2023/1000000, 2) as "2023",
       ROUND(y2024/1000000, 2) as "2024",
       ROUND(y2025/1000000, 2) as "2025",
       ROUND(total/1000000, 2) as "Total"
FROM company_group_by_year_tipo
WHERE group_name = 'FCC AQUALIA'  -- Change this to filter by company group
ORDER BY total DESC;
