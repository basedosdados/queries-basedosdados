SELECT
    sequencial, 
    CONCAT(ano,sequencial) as ano_sequencial_candidato,
    nome_urna, 
    sigla_partido, 
    INITCAP(cargo) as cargo, 
    sigla_uf,
    INITCAP(genero) as genero, 
    INITCAP(raca) as raca, 
    idade,
    CASE 
    WHEN SAFE_CAST(idade as INT64) < 29 THEN "18-29"
    WHEN SAFE_CAST(idade as INT64) < 50 THEN "30-49"
    WHEN SAFE_CAST(idade as INT64) < 70 THEN "50-69"
    WHEN SAFE_CAST(idade as INT64) >=70 THEN "70-100"
    END AS faixa_etaria
FROM `basedosdados.br_tse_eleicoes.candidatos` 
WHERE ano = 2022