{{ 
  config(
    schema='br_inep_saeb',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1995,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["sigla_uf"],
    labels = {'tema': 'educacao'})
}} 

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_regiao AS STRING) id_regiao,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_escola AS STRING) id_escola,
SAFE_CAST(rede AS STRING) rede,
SAFE_CAST(localizacao AS STRING) localizacao,
SAFE_CAST(serie AS INT64) serie,
SAFE_CAST(turno AS STRING) turno,
SAFE_CAST(disciplina AS STRING) disciplina,
SAFE_CAST(id_turma AS STRING) id_turma,
SAFE_CAST(id_aluno AS STRING) id_aluno,
SAFE_CAST(sexo AS STRING) sexo,
SAFE_CAST(raca_cor AS STRING) raca_cor,
SAFE_CAST(estrato AS INT64) estrato,
SAFE_CAST(amostra AS INT64) amostra,
SAFE_CAST(peso_aluno AS FLOAT64) peso_aluno,
SAFE_CAST(proficiencia AS FLOAT64) proficiencia,
SAFE_CAST(erro_padrao AS FLOAT64) erro_padrao,
SAFE_CAST(proficiencia_saeb AS FLOAT64) proficiencia_saeb,
SAFE_CAST(erro_padrao_saeb AS FLOAT64) erro_padrao_saeb
FROM basedosdados.br_inep_saeb_staging.proficiencia AS t