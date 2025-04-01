SELECT
    rc.Razao_Social,
    SUM(t.VL_SALDO_FINAL) AS total_despesa
FROM
    4T2024 t
JOIN
    relatorio_cadop rc ON t.REG_ANS = rc.Registro_ANS
WHERE
    t.CD_CONTA_CONTABIL = 471819019
GROUP BY
    rc.Razao_Social
ORDER BY
    total_despesa DESC
LIMIT 10;