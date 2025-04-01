-- Quais as 10 operadoras com maiores despesas nessa categoria no último ano? (2023/último ano)
SELECT
    Razao_Social,
    SUM(TOTAL_DESPESAS) AS TOTAL_DESPESAS
FROM (
    SELECT
        relatorio_cadop.Razao_Social AS Razao_Social,
        1t2023.VL_SALDO_FINAL AS TOTAL_DESPESAS
    FROM
        relatorio_cadop
    JOIN
        1t2023 ON relatorio_cadop.Registro_ANS = 1t2023.REG_ANS
    WHERE
        1t2023.DESCRICAO LIKE '%Eventos Sinistros%'
        OR 1t2023.DESCRICAO LIKE '%EVENTOSSINISTROS%'
        OR 1t2023.DESCRICAO LIKE '%EVENTOS INDENIZVEIS%'
    UNION ALL
    SELECT
        relatorio_cadop.Razao_Social AS Razao_Social,
        2t2023.VL_SALDO_FINAL AS TOTAL_DESPESAS
    FROM
        relatorio_cadop
    JOIN
        2t2023 ON relatorio_cadop.Registro_ANS = 2t2023.REG_ANS
    WHERE
        2t2023.DESCRICAO LIKE '%Eventos Sinistros%'
        OR 2t2023.DESCRICAO LIKE '%EVENTOSSINISTROS%'
        OR 2t2023.DESCRICAO LIKE '%EVENTOS INDENIZVEIS%'
    UNION ALL
    SELECT
        relatorio_cadop.Razao_Social AS Razao_Social,
        3t2023.VL_SALDO_FINAL AS TOTAL_DESPESAS
    FROM
        relatorio_cadop
    JOIN
        3t2023 ON relatorio_cadop.Registro_ANS = 3t2023.REG_ANS
    WHERE
        3t2023.DESCRICAO LIKE '%Eventos Sinistros%'
        OR 3t2023.DESCRICAO LIKE '%EVENTOSSINISTROS%'
        OR 3t2023.DESCRICAO LIKE '%EVENTOS INDENIZVEIS%'
    UNION ALL
    SELECT
        relatorio_cadop.Razao_Social AS Razao_Social,
        4t2023.VL_SALDO_FINAL AS TOTAL_DESPESAS
    FROM
        relatorio_cadop
    JOIN
        4t2023 ON relatorio_cadop.Registro_ANS = 4t2023.REG_ANS
    WHERE
        4t2023.DESCRICAO LIKE '%Eventos Sinistros%'
        OR 4t2023.DESCRICAO LIKE '%EVENTOSSINISTROS%'
        OR 4t2023.DESCRICAO LIKE '%EVENTOS INDENIZVEIS%'
) AS todas_despesas
GROUP BY
    Razao_Social
ORDER BY
    TOTAL_DESPESAS DESC
LIMIT 10;