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
SAFE_CAST(id_regiao AS STRING) id_regiao,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(area AS STRING) area,
SAFE_CAST(id_escola AS STRING) id_escola,
SAFE_CAST(rede AS STRING) rede,
SAFE_CAST(localizacao AS STRING) localizacao,
SAFE_CAST(id_turma AS STRING) id_turma,
SAFE_CAST(turno AS STRING) turno,
SAFE_CAST(serie AS INT64) serie,
SAFE_CAST(id_aluno AS STRING) id_aluno,
SAFE_CAST(situacao_censo AS INT64) situacao_censo,
SAFE_CAST(disciplina AS STRING) disciplina,
SAFE_CAST(preenchimento_caderno AS INT64) preenchimento_caderno,
SAFE_CAST(presenca AS INT64) presenca,
SAFE_CAST(caderno AS STRING) caderno,
SAFE_CAST(bloco_1 AS STRING) bloco_1,
SAFE_CAST(bloco_2 AS STRING) bloco_2,
SAFE_CAST(bloco_1_aberto AS STRING) bloco_1_aberto,
SAFE_CAST(bloco_2_aberto AS STRING) bloco_2_aberto,
SAFE_CAST(respostas_bloco_1 AS STRING) respostas_bloco_1,
SAFE_CAST(respostas_bloco_2 AS STRING) respostas_bloco_2,
SAFE_CAST(conceito_q1 AS STRING) conceito_q1,
SAFE_CAST(conceito_q2 AS STRING) conceito_q2,
SAFE_CAST(resposta_texto AS STRING) resposta_texto,
SAFE_CAST(conceito_proposito AS STRING) conceito_proposito,
SAFE_CAST(conceito_elemento AS STRING) conceito_elemento,
SAFE_CAST(conceito_segmentacao AS STRING) conceito_segmentacao,
SAFE_CAST(texto_grafia AS STRING) texto_grafia,
SAFE_CAST(indicador_proficiencia AS STRING) indicador_proficiencia,
SAFE_CAST(amostra AS STRING) amostra,
SAFE_CAST(estrato AS STRING) estrato,
SAFE_CAST(peso_aluno AS FLOAT64) peso_aluno,
SAFE_CAST(proficiencia AS FLOAT64) proficiencia,
SAFE_CAST(erro_padrao AS FLOAT64) erro_padrao,
SAFE_CAST(proficiencia_saeb AS FLOAT64) proficiencia_saeb,
SAFE_CAST(erro_padrao_saeb AS FLOAT64) erro_padrao_saeb
FROM basedosdados-staging.br_inep_saeb_staging.aluno_ef_2ano AS t