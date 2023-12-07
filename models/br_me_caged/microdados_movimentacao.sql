{{ 
  config(
    schema='br_me_caged',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2020,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["mes", "sigla_uf"],
    labels = {'project_id': 'basedosdados', 'tema': 'economia'},
    pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    post_hook = [
        'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(DATE(2023,12,6),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6 OR  DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 0)',
        'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (DATE_DIFF(DATE(2023,12,6),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) < 6 OR  DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) < 0)']              )
 }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(a.sigla_uf AS STRING) sigla_uf,
SAFE_CAST(b.id_municipio AS STRING) id_municipio,
SAFE_CAST(cnae_2_secao AS STRING) cnae_2_secao,
SAFE_CAST(cnae_2_subclasse AS STRING) cnae_2_subclasse,
SAFE_CAST(saldo_movimentacao AS INT64) saldo_movimentacao,
SAFE_CAST(cbo_2002 AS STRING) cbo_2002,
SAFE_CAST(categoria AS STRING) categoria,
SAFE_CAST(grau_instrucao AS STRING) grau_instrucao,
SAFE_CAST(REPLACE(idade,'.0','') AS INT64) idade,
CASE
  WHEN horas_contratuais LIKE '%,%' THEN CAST(REPLACE(horas_contratuais, ',', '.') AS FLOAT64)
  ELSE CAST(horas_contratuais AS FLOAT64)
END AS horas_contratuais,
SAFE_CAST(raca_cor AS STRING) raca_cor,
SAFE_CAST(sexo AS STRING) sexo,
SAFE_CAST(tipo_empregador AS STRING) tipo_empregador,
SAFE_CAST(tipo_estabelecimento AS STRING) tipo_estabelecimento,
SAFE_CAST(tipo_movimentacao AS STRING) tipo_movimentacao,
SAFE_CAST(tipo_deficiencia AS STRING) tipo_deficiencia,
SAFE_CAST(indicador_trabalho_intermitente AS STRING) indicador_trabalho_intermitente,
SAFE_CAST(indicador_trabalho_parcial AS STRING) indicador_trabalho_parcial,
SAFE_CAST(REPLACE(salario_mensal,',','.') AS FLOAT64) salario_mensal,
SAFE_CAST(tamanho_estabelecimento_janeiro AS STRING) tamanho_estabelecimento_janeiro,
SAFE_CAST(indicador_aprendiz AS STRING) indicador_aprendiz,
SAFE_CAST(origem_informacao AS STRING) origem_informacao,
SAFE_CAST(indicador_fora_prazo AS INT64) indicador_fora_prazo
FROM `basedosdados-staging.br_me_caged_staging.microdados_movimentacao` a
LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` b
  ON a.id_municipio =  b.id_municipio_6
