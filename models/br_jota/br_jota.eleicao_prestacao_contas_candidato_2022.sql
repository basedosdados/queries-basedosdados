WITH soma_receitas_candidato AS (
SELECT
    sequencial_candidato,
    SUM(CASE WHEN origem = 'Recursos De Partido Politico' THEN valor END) as receita_partido,
    SUM(valor) AS valor_total
FROM `basedosdados-perguntas.br_jota.eleicao_prestacao_contas_candidato_origem_2022`
WHERE sequencial_candidato IS NOT NULL
AND receita_despesa = 'Receita'
GROUP BY 1
)

SELECT
candidato_info.*,
RANK() OVER (PARTITION BY cargo ORDER BY valor_total DESC ) AS rank_cargo,
RANK() OVER (PARTITION BY sigla_partido ORDER BY valor_total DESC ) AS rank_partido,
RANK() OVER (PARTITION BY sigla_partido,cargo ORDER BY valor_total DESC ) AS rank_cargo_partido,
valores.* EXCEPT (sequencial_candidato)
FROM `basedosdados-perguntas.br_jota.eleicao_perfil_candidato_2022` AS candidato_info
LEFT JOIN soma_receitas_candidato AS valores
ON candidato_info.sequencial = valores.sequencial_candidato