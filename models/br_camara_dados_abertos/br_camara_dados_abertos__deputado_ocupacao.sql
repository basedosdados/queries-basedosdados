{{ config(alias='deputado_ocupacao',schema='br_camara_dados_abertos') }}
SELECT
    SAFE_CAST(ano_inicio AS INT64) ano_inicio,
    SAFE_CAST(ano_fim AS INT64) ano_fim,
    SAFE_CAST(id_deputado AS STRING) id_deputado,
    SAFE_CAST(sigla_uf AS STRING) sigla_uf,
    SAFE_CAST(entidade AS STRING) entidade,
    SAFE_CAST(titulo AS STRING) titulo,
FROM basedosdados-staging.br_camara_dados_abertos_staging.deputado_ocupacao AS t