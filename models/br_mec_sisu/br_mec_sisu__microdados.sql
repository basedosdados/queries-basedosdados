{{ 
  config(
    schema='br_mec_sisu',
    alias = 'microdados',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2017,
        "end": 2024,
        "interval": 1}
    },
    cluster_by = ["ano", "sigla_uf_candidato"],
    labels = {'tema': 'educacao'})
}}

SELECT
  SAFE_CAST (ano AS INT64) AS ano,
  SAFE_CAST (edicao AS STRING) AS edicao,
  SAFE_CAST (etapa AS STRING) AS etapa,
  SAFE_CAST (sigla_uf_ies AS STRING) AS sigla_uf_ies,
  SAFE_CAST (id_ies AS STRING) AS id_ies,
  SAFE_CAST (sigla_ies AS STRING) AS sigla_ies,
  SAFE_CAST (sigla_uf_campus AS STRING) AS sigla_uf_campus,
  SAFE_CAST (id_municipio AS STRING) AS id_municipio_campus,
  SAFE_CAST (id_campus AS STRING) AS id_campus,
  SAFE_CAST (campus AS STRING) AS campus,
  SAFE_CAST (id_curso AS STRING) AS id_curso,
  CASE
    WHEN turno = 'Integral'    THEN '1'
    WHEN turno = 'Matutino'    THEN '2'
    WHEN turno = 'Vespertino'  THEN '3'
    WHEN turno = 'Noturno'     THEN '4'
    WHEN turno = 'EaD'         THEN '5'
  END AS turno,
  CASE
    WHEN periodicidade = 'Trimestral'       THEN '3'
    WHEN periodicidade = 'Quadrimestral'    THEN '4'
    WHEN periodicidade = 'Quadrimestral'    THEN '6'
    WHEN periodicidade = 'Anual'            THEN '12'
  END AS periodicidade,
  SAFE_CAST (tipo_cota AS STRING) AS tipo_cota,
  SAFE_CAST (ds_modalidade_concorrencia AS STRING) AS modalidade_concorrencia,
  SAFE_CAST (quantidade_vagas_concorrencia AS INT64) AS quantidade_vagas_concorrencia,
  SAFE_CAST (percentual_bonus AS FLOAT64) AS percentual_bonus,
  SAFE_CAST (peso_l AS FLOAT64) AS peso_l,
  SAFE_CAST (peso_ch AS FLOAT64) AS peso_ch,
  SAFE_CAST (peso_cn AS FLOAT64) AS peso_cn,
  SAFE_CAST (peso_m AS FLOAT64) AS peso_m,
  SAFE_CAST (peso_r AS FLOAT64) AS peso_r,
  SAFE_CAST (nota_minima_l AS FLOAT64) AS nota_minima_l,
  SAFE_CAST (nota_minima_ch AS FLOAT64) AS nota_minima_ch,
  SAFE_CAST (nota_minima_cn AS FLOAT64) AS nota_minima_cn,
  SAFE_CAST (nota_minima_m AS FLOAT64) AS nota_minima_m,
  SAFE_CAST (nota_minima_r AS FLOAT64) AS nota_minima_r,
  SAFE_CAST (media_minima AS FLOAT64) AS media_minima,
  SAFE_CAST (cpf AS STRING) AS cpf,
  SAFE_CAST (inscricao_enem AS STRING) AS inscricao_enem,
  SAFE_CAST (candidato AS STRING) AS candidato,
  SAFE_CAST (sexo AS STRING) AS sexo,
  CASE WHEN ((LENGTH(data_nascimento) = 8 ) AND (CAST(SUBSTR(data_nascimento,1,2) AS INT64) > 30)) THEN CONCAT('19', data_nascimento)
       WHEN ((LENGTH(data_nascimento) = 8 ) AND (CAST(SUBSTR(data_nascimento,1,2) AS INT64) < 30)) THEN CONCAT('20', data_nascimento)
       ELSE data_nascimento
  END AS data_nascimento,
  SAFE_CAST (sigla_uf_candidato AS STRING) AS sigla_uf_candidato,
  SAFE_CAST (id_municipio AS STRING) AS id_municipio_candidato,
  SAFE_CAST (opcao AS STRING) AS opcao,
  SAFE_CAST (nota_l AS FLOAT64) AS nota_l,
  SAFE_CAST (nota_ch AS FLOAT64) AS nota_ch,
  SAFE_CAST (nota_cn AS FLOAT64) AS nota_cn,
  SAFE_CAST (nota_m AS FLOAT64) AS nota_m,
  SAFE_CAST (nota_r AS FLOAT64) AS nota_r,
  SAFE_CAST (nota_l_peso AS FLOAT64) AS nota_l_peso,
  SAFE_CAST (nota_ch_peso AS FLOAT64) AS nota_ch_peso,
  SAFE_CAST (nota_cn_peso AS FLOAT64) AS nota_cn_peso,
  SAFE_CAST (nota_m_peso AS FLOAT64) AS nota_m_peso,
  SAFE_CAST (nota_r_peso AS FLOAT64) AS nota_r_peso,  
  SAFE_CAST (nota_candidato AS FLOAT64) AS nota_candidato, 
  SAFE_CAST (nota_corte AS FLOAT64) AS nota_corte, 
  SAFE_CAST (classificacao AS INT64) AS classificacao, 
  SAFE_CAST ((CASE 
                WHEN status_aprovado = 'N' THEN False
                WHEN status_aprovado = 'S' THEN True 
              END) AS BOOL) AS status_aprovado, 
  CASE 
    WHEN status_matricula = 'CANCELADA'                                 THEN '1'
    WHEN status_matricula = 'DOCUMENTACAO REJEITADA'                    THEN '2'
    WHEN status_matricula = 'DOCUMENTAÇÃO REJEITADA'                    THEN '2'
    WHEN status_matricula = 'EFETIVADA'                                 THEN '3'
    WHEN status_matricula = 'NÃO COMPARECEU'                            THEN '4'
    WHEN status_matricula = 'NÃO CONVOCADO'                             THEN '5'
    WHEN status_matricula = 'PENDENTE'                                  THEN '6'
    WHEN status_matricula = 'SUBSTITUIDA - FORA DO PRAZO'               THEN '7'
    WHEN status_matricula = 'SUBSTITUIDA - MATRICULA FORA DO PRAZO'     THEN '7'
    WHEN status_matricula = 'SUBSTITUIDA - MESMA IES'                   THEN '8'
    WHEN status_matricula = 'SUBSTITUIDA - OUTRA IES'                   THEN '9'
    WHEN status_matricula = 'SUBSTITUÍDA MESMA IES'                     THEN '8'
    WHEN status_matricula = 'SUBSTITUÍDA OUTRA IES'                     THEN '9'
  END AS status_matricula
FROM `basedosdados-dev.br_mec_sisu_staging.microdados` s
LEFT JOIN `basedosdados-dev.br_bd_diretorios_brasil.municipio` d ON LOWER(s.nome_municipio_campus) = LOWER(d.nome) 
                                                                 AND LOWER(s.nome_municipio_candidato) = LOWER(d.nome)