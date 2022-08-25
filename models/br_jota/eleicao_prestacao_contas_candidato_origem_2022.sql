WITH despesa AS (
    SELECT 
      sequencial_candidato,
      CONCAT(ano,sequencial_candidato) as ano_sequencial_candidato,
      'Despesa' AS receita_despesa,
      INITCAP(origem_despesa) AS origem,
      SUM(valor_despesa) AS valor
    FROM `basedosdados.br_tse_eleicoes.despesas_candidato`
    WHERE ano = 2022
    GROUP BY 1,2,3,4
  ),

  receita AS (
    SELECT   
      sequencial_candidato,
      CONCAT(ano,sequencial_candidato) as ano_sequencial_candidato,
      'Receita' AS receita_despesa,
      INITCAP(origem_receita) AS origem, 
      SUM(valor_receita) AS valor 
    FROM `basedosdados.br_tse_eleicoes.receitas_candidato` 
    WHERE ano = 2022 
    GROUP BY 1,2,3,4
  ),

  receita_despesa AS (
    SELECT *
    FROM despesa
    UNION ALL 
    SELECT *
    FROM receita)

SELECT t1.*,
t2.categoria
FROM receita_despesa as t1
LEFT JOIN `basedosdados-perguntas.br_jota.eleicao_auxiliar_categoria_origem` as t2
ON t1.origem = t2.origem
