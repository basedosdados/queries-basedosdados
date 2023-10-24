{{ 
  config(
    schema='world_wb_mides',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1996,
        "end": 2022,
        "interval": 1}
    },
    cluster_by = ["mes", "sigla_uf"],
    labels = {'project_id': 'basedosdados-dev', 'tema': 'economia'},
    post_hook=['REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
                'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"'])
 }}
SELECT
  ano                    INT64,
  mes                    INT64,
  data                   DATE,
  sigla_uf               STRING,
  id_municipio           STRING,
  orgao                  STRING,
  id_unidade_gestora     STRING,
  id_empenho_bd          STRING,
  id_empenho             STRING,
  numero_empenho         STRING,
  id_liquidacao_bd       STRING,
  id_liquidacao          STRING,
  numero_liquidacao      STRING,
  id_pagamento_bd        STRING,
  id_pagamento           STRING,
  numero                 STRING,
  nome_credor            STRING,
  documento_credor       STRING,
  indicador_restos_pagar BOOL,
  fonte                  STRING,
  valor_inicial          FLOAT64,
  valor_anulacao         FLOAT64,
  valor_ajuste           FLOAT64,
  valor_final            FLOAT64,
  valor_liquido_recebido FLOAT64
FROM(
WITH empenho_ce AS (
  SELECT
    SAFE_CAST (CONCAT(numero_empenho, ' ', TRIM(codigo_orgao), ' ', TRIM(codigo_unidade), ' ', m.geoibgeId, ' ', (SUBSTRING(data_emissao_empenho,6,2)), ' ', (SUBSTRING(data_emissao_empenho,3,2))) AS STRING) AS id_empenho_bd,
    SAFE_CAST (nome_negociante AS STRING) AS nome_credor,
    SAFE_CAST (REPLACE (REPLACE (numero_documento_negociante, '.',''), '-','') AS STRING) AS documento_credor,
    SAFE_CAST (SAFE_CAST (codigo_fonte_ AS INT64) AS STRING) AS fonte,
  FROM basedosdados-dev.world_wb_mides_staging.raw_empenho_ce e
  LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_ce m ON e.codigo_municipio = m.codigo_municipio
),
  pago_ce AS (
    SELECT
      (SAFE_CAST(EXTRACT(YEAR FROM DATE(data_nota_pagamento)) AS INT64)) AS ano,
      (SAFE_CAST(EXTRACT(MONTH FROM DATE(data_nota_pagamento)) AS INT64)) AS mes,
      SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(data_nota_pagamento)) AS DATE) AS data,
      'CE' AS sigla_uf,
      SAFE_CAST (m.geoibgeId AS STRING) AS id_municipio,
      SAFE_CAST (p.codigo_orgao AS STRING) orgao,
      SAFE_CAST (p.codigo_unidade AS STRING) AS id_unidade_gestora,
      SAFE_CAST (CONCAT(p.numero_empenho, ' ', TRIM(p.codigo_orgao), ' ', TRIM(p.codigo_unidade), ' ', m.geoibgeId, ' ', (SUBSTRING(p.data_emissao_empenho,6,2)), ' ', (SUBSTRING(p.data_emissao_empenho,3,2))) AS STRING) AS id_empenho_bd,
      SAFE_CAST (NULL AS STRING) AS id_empenho,
      SAFE_CAST (p.numero_empenho AS STRING) AS numero_empenho,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao,
      SAFE_CAST (NULL AS STRING) AS numero_liquidacao,
      SAFE_CAST (CONCAT(p.numero_empenho, ' ', SAFE_CAST(SAFE_CAST (numero_nota_pagamento AS INT64) AS STRING), ' ', TRIM(p.codigo_orgao), ' ', TRIM(p.codigo_unidade), ' ', m.geoibgeId, ' ', (SUBSTRING(p.data_emissao_empenho,6,2)), ' ', (SUBSTRING(p.data_emissao_empenho,3,2))) AS STRING) AS id_pagamento_bd,
      SAFE_CAST (NULL AS STRING) AS id_pagamento,
      SAFE_CAST (numero_nota_pagamento AS STRING) AS numero,
      SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
      ROUND(SAFE_CAST (valor_nota_pagamento AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (valor_nota_pagamento AS FLOAT64),2) AS valor_final,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_liquido_recebido,
    FROM basedosdados-dev.world_wb_mides_staging.raw_pagamento_ce p
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_ce m ON p.codigo_municipio = m.codigo_municipio
),
  frequencia_ce AS (
    SELECT
      id_pagamento_bd, COUNT(id_pagamento_bd) AS frequencia_id
    FROM pago_ce
    GROUP BY 1
),
  pagamento_ce AS (
    SELECT
      ano,
      mes,
      data,
      sigla_uf,
      id_municipio,
      orgao,
      id_unidade_gestora,
      p.id_empenho_bd,
      p.id_empenho,
      p.numero_empenho,
      id_liquidacao_bd,
      id_liquidacao,
      numero_liquidacao,
      (CASE WHEN (frequencia_id > 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_pagamento_bd END) AS id_pagamento_bd,
      id_pagamento,
      numero,
      nome_credor,
      documento_credor,
      indicador_restos_pagar,
      fonte,
      valor_inicial,
      valor_anulacao,
      valor_ajuste,
      valor_final,
      valor_liquido_recebido
    FROM pago_ce p
    LEFT JOIN frequencia_ce f ON p.id_pagamento_bd = f.id_pagamento_bd
    LEFT JOIN empenho_ce e ON p.id_empenho_bd = e.id_empenho_bd
),
  pagamento_mg AS (
  SELECT DISTINCT
    SAFE_CAST (p.ano AS INT64) AS ano,
    SAFE_CAST (p.mes AS INT64) AS mes,
    SAFE_CAST (p.data AS DATE) AS data,
    SAFE_CAST (p.sigla_uf AS STRING) AS sigla_uf,
    SAFE_CAST (p.id_municipio AS STRING) AS id_municipio,
    SAFE_CAST (p.orgao AS STRING) AS orgao,
    SAFE_CAST (p.id_unidade_gestora AS STRING) AS id_unidade_gestora,
    SAFE_CAST (CASE
      WHEN id_empenho != '-1' THEN CONCAT(id_empenho, ' ', p.orgao, ' ', p.id_municipio, ' ', (RIGHT(ano,2)))
      WHEN id_empenho = '-1'  THEN CONCAT(id_empenho_origem, ' ', r.orgao, ' ', r.id_municipio, ' ', (RIGHT(num_ano_emp_origem,2)))
      END AS STRING) AS id_empenho_bd,
    SAFE_CAST (CASE
      WHEN p.id_empenho = '-1' THEN REPLACE (p.id_empenho, '-1', id_empenho_origem) END AS STRING) AS id_empenho,
    SAFE_CAST (p.numero_empenho AS STRING) AS numero_empenho,
    SAFE_CAST (CASE
      WHEN p.id_liquidacao != '-1' THEN CONCAT(p.id_liquidacao, ' ', p.orgao, ' ', p.id_municipio, ' ', (RIGHT(p.ano,2)))
      WHEN p.id_liquidacao = '-1'  THEN CONCAT(' ', r.orgao, ' ', r.id_municipio, ' ', (RIGHT(p.ano,2)))
    END AS STRING) AS id_liquidacao_bd,
    SAFE_CAST (CASE
      WHEN p.id_empenho = '-1' THEN REPLACE (p.id_liquidacao, '-1', '') END AS STRING) AS id_liquidacao,
    SAFE_CAST (p.numero_liquidacao AS STRING) AS numero_liquidacao,
    SAFE_CAST (CONCAT(id_pagamento, ' ', p.orgao, ' ', p.id_municipio, ' ', (RIGHT(p.ano,2))) AS STRING) AS id_pagamento_bd,
    SAFE_CAST (id_pagamento AS STRING) AS id_pagamento,
    SAFE_CAST (p.numero_pagamento AS STRING) AS numero,
    SAFE_CAST (nome_credor AS STRING) AS nome_credor,
    SAFE_CAST (REPLACE(REPLACE (documento_credor, '.', ''), '-','') AS STRING) AS documento_credor,
    SAFE_CAST (CASE WHEN p.id_rsp != '-1' THEN 1 ELSE 0 END AS BOOL) AS indicador_restos_pagar,
    SAFE_CAST (LEFT(fonte,3) AS STRING) AS fonte,
    ROUND(SAFE_CAST (valor_pagamento_original AS FLOAT64),2) AS valor_inicial,
    ROUND(IFNULL(SAFE_CAST (vlr_anu_fonte AS FLOAT64),0),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SAFE_CAST (valor_pagamento_original AS FLOAT64) - IFNULL(SAFE_CAST (vlr_anu_fonte AS FLOAT64),0),2) AS valor_final,
    ROUND(SAFE_CAST (valor_pagamento_original AS FLOAT64) - IFNULL(SAFE_CAST (vlr_anu_fonte AS FLOAT64),0) -  IFNULL(SAFE_CAST (vlr_ret_fonte AS FLOAT64),0),2) AS valor_liquido_recebido,
  FROM basedosdados-dev.world_wb_mides_staging.raw_pagamento_mg AS p
  LEFT JOIN basedosdados-dev.world_wb_mides_staging.raw_rsp_mg AS r ON p.id_rsp=r.id_rsp
),
pago_pb AS (
    SELECT
      SAFE_CAST (p.dt_Ano AS INT64) AS ano,
      SAFE_CAST(SUBSTRING(TRIM(dt_pagamento),-7,2) AS INT64) AS mes,
      SAFE_CAST (CONCAT(SUBSTRING(TRIM(dt_pagamento),-4),'-',SUBSTRING(TRIM(dt_pagamento),-7,2),'-',SUBSTRING(TRIM(dt_pagamento),1,2))AS DATE) AS data,
      m.sigla_uf,
      SAFE_CAST (m.id_municipio AS STRING) AS id_municipio,
      SAFE_CAST (NULL AS STRING) AS  orgao,
      SAFE_CAST (p.cd_UGestora AS STRING) AS id_unidade_gestora,
      SAFE_CAST (CONCAT(e.nu_Empenho, ' ', e.cd_UGestora, ' ', m.id_municipio, ' ', (RIGHT(e.dt_Ano,2))) AS STRING) AS id_empenho_bd,
      SAFE_CAST (NULL AS STRING) AS  id_empenho,
      SAFE_CAST (p.nu_Empenho AS STRING) AS numero_empenho,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao,
      SAFE_CAST (NULL AS STRING) AS numero_liquidacao,
      SAFE_CAST (CONCAT(p.nu_Empenho, ' ', (SAFE_CAST (nu_Parcela AS INT64)), ' ', p.cd_UGestora, ' ', id_municipio, ' ', (RIGHT(p.dt_Ano,2))) AS STRING) AS id_pagamento_bd,
      SAFE_CAST (NULL AS STRING) AS id_pagamento,
      SAFE_CAST (nu_Parcela AS STRING) AS numero,
      SAFE_CAST (no_Credor AS STRING) AS nome_credor,
      SAFE_CAST (REPLACE (REPLACE (cd_credor, '.', ''), '-','') AS STRING) AS documento_credor,
      SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
      SAFE_CAST (tp_FonteRecursos AS STRING) AS fonte,
      ROUND(SAFE_CAST (vl_Pagamento AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2)AS valor_anulacao,
      ROUND(SAFE_CAST (vl_Retencao AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (vl_Pagamento AS FLOAT64),2) AS valor_final,
      ROUND(SAFE_CAST (vl_Pagamento AS FLOAT64) - SAFE_CAST (vl_Retencao AS FLOAT64),2) AS valor_liquido_recebido,
    FROM basedosdados-dev.world_wb_mides_staging.raw_pagamento_pb p
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.raw_empenho_pb e ON p.nu_Empenho = e.nu_Empenho AND p.cd_UGestora = e.cd_ugestora AND p.de_UOrcamentaria = e.de_UOrcamentaria AND p.dt_Ano = e.dt_Ano
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pb m ON SAFE_CAST(e.cd_ugestora AS STRING) = SAFE_CAST(m.id_unidade_gestora AS STRING)
),
  frequencia_pb AS (
    SELECT id_pagamento_bd, COUNT (id_pagamento_bd) frequencia_id FROM pago_pb
    GROUP BY 1
),
  pagamento_pb AS (
    SELECT
      ano,
      mes,
      data,
      sigla_uf,
      id_municipio,
      orgao,
      id_unidade_gestora,
      CASE WHEN (frequencia_id > 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_empenho_bd END AS id_empenho_bd,
      id_empenho,
      numero_empenho,
      id_liquidacao_bd,
      id_liquidacao,
      numero_liquidacao,
      CASE WHEN (frequencia_id > 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_pagamento_bd END AS id_pagamento_bd,
      id_pagamento,
      numero,
      nome_credor,
      documento_credor,
      indicador_restos_pagar,
      fonte,
      valor_inicial,
      valor_anulacao,
      valor_ajuste,
      valor_final,
      valor_liquido_recebido
    FROM pago_pb p
    LEFT JOIN frequencia_pb f ON p.id_pagamento_bd = f.id_pagamento_bd
),
  pagamento_pe AS (
    SELECT
      SAFE_CAST (p.ANOREFERENCIA AS INT64) AS ano,
      (SAFE_CAST(EXTRACT(MONTH FROM DATE(DATA)) AS INT64)) AS mes,
      SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(DATA)) AS DATE) AS data,
      SAFE_CAST (UNIDADEFEDERATIVA AS STRING) AS sigla_uf,
      SAFE_CAST (CODIGOIBGE AS STRING) AS id_municipio,
      SAFE_CAST (NULL AS STRING) orgao,
      SAFE_CAST (ID_UNIDADEGESTORA AS STRING) AS id_unidade_gestora,
      SAFE_CAST (NULL AS STRING) AS id_empenho_bd,
      SAFE_CAST (TRIM(IDEMPENHO) AS STRING) AS id_empenho,
      SAFE_CAST (p.NUMEROEMPENHO AS STRING) AS numero_empenho,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao,
      SAFE_CAST (NULL AS STRING) AS numero_liquidacao,
      SAFE_CAST (NULL AS STRING) AS id_pagamento_bd,
      SAFE_CAST (NULL AS STRING) AS id_pagamento,
      SAFE_CAST (NULL AS STRING) AS numero,
      SAFE_CAST (NULL AS STRING) AS nome_credor,
      SAFE_CAST (NULL AS STRING) AS documento_credor,
      SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
      SAFE_CAST (NULL AS STRING) AS fonte,
      ROUND(SAFE_CAST (VALOR AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND((CASE WHEN (SAFE_CAST ((VALOR) AS FLOAT64) < -1000000000000) THEN NULL ELSE SAFE_CAST ((VALOR) AS FLOAT64) END),2) AS valor_final,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_liquido_recebido,
    FROM basedosdados-dev.world_wb_mides_staging.raw_pagamento_pe p
    INNER JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pe m ON SAFE_CAST(p.ID_UNIDADE_GESTORA AS STRING) = SAFE_CAST(m.ID_UNIDADEGESTORA AS STRING)
),
  pagamento_pr AS (
      SELECT
    SAFE_CAST (nrAnoPagamento AS INT64) AS ano,
    (SAFE_CAST(EXTRACT(MONTH FROM DATE (dtOperacao)) AS INT64)) AS mes,
    SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(dtOperacao)) AS DATE) AS data,
    sigla_uf,
    id_municipio,
    SAFE_CAST (cdOrgao AS STRING) AS orgao,
    SAFE_CAST (cdUnidade AS STRING) AS id_unidade_gestora,
    SAFE_CAST (CONCAT(p.idEmpenho, ' ', m.id_municipio) AS STRING) AS id_empenho_bd,
    SAFE_CAST (p.idEmpenho AS STRING) AS id_empenho,
    SAFE_CAST (nrEmpenho AS STRING) AS numero_empenho,
    SAFE_CAST (CONCAT(p.idLiquidacao,' ', m.id_municipio) AS STRING) AS id_liquidacao_bd,
    SAFE_CAST (p.idLiquidacao AS STRING) AS id_liquidacao,
    SAFE_CAST (NULL AS STRING) AS numero_liquidacao,
    SAFE_CAST (CONCAT(p.idPagamento,' ', m.id_municipio) AS STRING) AS id_pagamento_bd,
    SAFE_CAST (idPagamento AS STRING) AS id_pagamento,
    SAFE_CAST (nrPagamento AS STRING) AS numero,
    SAFE_CAST (nmCredor AS STRING) AS nome_credor,
    SAFE_CAST (REGEXP_REPLACE(nrDocCredor, '[^0-9]', '') AS STRING) AS documento_credor,
    SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
    SAFE_CAST (cdFonteReceita AS STRING) AS fonte,
    ROUND(SAFE_CAST (vlOperacao AS FLOAT64),2) AS valor_inicial,
    ROUND(SAFE_CAST (nrAnoLiquidacao AS FLOAT64),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SAFE_CAST (p.cdIBGE AS FLOAT64),2) AS valor_final,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_liquido_recebido,
  FROM basedosdados-dev.world_wb_mides_staging.raw_pagamento_pr p
  LEFT JOIN basedosdados-dev.world_wb_mides_staging.raw_empenho_pr e ON p.idEmpenho = e.idEmpenho
  LEFT JOIN basedosdados.br_bd_diretorios_brasil.municipio m ON e.cdIBGE = id_municipio_6
),
  pago_rs AS (
    SELECT
      MIN(ano_recebimento) AS ano_recebimento,
      SAFE_CAST(ano_operacao AS INT64) AS ano,
      SAFE_CAST(EXTRACT(MONTH FROM DATE(dt_operacao)) AS INT64) AS mes,
      SAFE_CAST(CONCAT(SUBSTRING(dt_operacao,1,4), '-', SUBSTRING(dt_operacao,6,2),  '-', SUBSTRING(dt_operacao,9,2)) AS DATE) AS data,
      m.sigla_uf AS sigla_uf,
      SAFE_CAST(a.id_municipio AS STRING) AS id_municipio,
      SAFE_CAST(c.cd_orgao AS STRING) AS orgao,
      SAFE_CAST(cd_orgao_orcamentario AS STRING) AS id_unidade_gestora,
      SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
      SAFE_CAST(NULL AS STRING) AS id_empenho,
      SAFE_CAST(nr_empenho AS STRING) AS numero_empenho,
      SAFE_CAST(CONCAT(nr_empenho, ' ', nr_liquidacao, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao,
      SAFE_CAST (nr_liquidacao AS STRING) AS numero_liquidacao,
      SAFE_CAST(CONCAT(nr_empenho, ' ', nr_liquidacao, ' ', nr_pagamento, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_pagamento_bd,
      SAFE_CAST (NULL AS STRING) AS id_pagamento,
      SAFE_CAST (nr_pagamento AS STRING) AS numero,
      SAFE_CAST (nm_credor AS STRING) AS nome_credor,
      SAFE_CAST (cnpj_cpf AS STRING) AS documento_credor,
      SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
      SAFE_CAST (NULL AS STRING) AS fonte,
      SAFE_CAST(vl_pagamento AS FLOAT64) AS valor_inicial
    FROM `basedosdados-dev.world_wb_mides_staging.raw_despesa_rs` AS c
    LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
    LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
    WHERE tipo_operacao = 'P' AND (SAFE_CAST(vl_pagamento AS FLOAT64) >= 0)
    GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
),
  estorno_rs AS (
    SELECT
      SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
      -1*SUM(SAFE_CAST(vl_pagamento AS FLOAT64)) AS valor_anulacao
    FROM `basedosdados-dev.world_wb_mides_staging.raw_despesa_rs` AS c
    LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
    LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
    WHERE tipo_operacao = 'P' AND (SAFE_CAST(vl_pagamento AS FLOAT64) < 0)
    GROUP BY 1
),
  frequencia_rs AS (
    SELECT
      id_empenho_bd, COUNT(id_empenho_bd) AS frequencia_id
    FROM pago_rs
    GROUP BY 1
  ),
    pagamento1_rs AS (
      SELECT
        ano,
        mes,
        data,
        sigla_uf,
        id_municipio,
        orgao,
        id_unidade_gestora,
        p.id_empenho_bd,
        id_empenho,
        numero_empenho,
        p.id_liquidacao_bd,
        id_liquidacao,
        numero_liquidacao,
        id_pagamento_bd,
        id_pagamento,
        numero,
        nome_credor,
        IFNULL(documento_credor, '99999999999') AS documento_credor,
        indicador_restos_pagar,
        fonte,
        ROUND(SUM(valor_inicial),2) AS valor_inicial,
        ROUND(SUM(valor_anulacao/frequencia_id),2) AS valor_anulacao,
        ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
        ROUND(SUM(valor_inicial - IFNULL((valor_anulacao/frequencia_id), 0)),2) AS valor_final,
        ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_liquido_recebido
      FROM pago_rs p
      LEFT JOIN estorno_rs e ON p.id_empenho_bd=e.id_empenho_bd
      LEFT JOIN frequencia_rs f ON p.id_empenho_bd=f.id_empenho_bd
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),
  ddata_rs AS (
    SELECT
      id_pagamento_bd,
      CASE WHEN (COUNT (DISTINCT data)) > 1 THEN 1 ELSE 0 END AS ddata
    FROM pagamento1_rs
    GROUP BY 1
),
  dorgao_rs AS (
    SELECT
      id_pagamento_bd,
      CASE WHEN (COUNT (DISTINCT orgao)) > 1 THEN 1 ELSE 0 END AS dorgao
    FROM pagamento1_rs
    GROUP BY 1
),
  dugest_rs AS (
    SELECT
      id_pagamento_bd,
      CASE WHEN (COUNT (DISTINCT id_unidade_gestora)) > 1 THEN 1 ELSE 0 END AS dugest
    FROM pagamento1_rs
    GROUP BY 1
),
  credor_rs AS (
    SELECT
      id_pagamento_bd,
      CASE WHEN (COUNT (DISTINCT nome_credor)) > 1 THEN 1 ELSE 0 END AS dcredor
    FROM pagamento1_rs
    GROUP BY 1
),
  dcredor_rs AS (
    SELECT
      id_pagamento_bd,
      CASE WHEN (COUNT (DISTINCT documento_credor)) > 1 THEN 1 ELSE 0 END AS ddocumento
    FROM pagamento1_rs
    GROUP BY 1
),
  dummies AS (
    SELECT
      d.id_pagamento_bd,
      ddata,
      dorgao,
      dugest,
      dcredor,
      ddocumento
    FROM ddata_rs d
    LEFT JOIN credor_rs c ON d.id_pagamento_bd = c.id_pagamento_bd
    LEFT JOIN dcredor_rs dc ON d.id_pagamento_bd = dc.id_pagamento_bd
    LEFT JOIN dorgao_rs o ON d.id_pagamento_bd = o.id_pagamento_bd
    LEFT JOIN dugest_rs u ON d.id_pagamento_bd = u.id_pagamento_bd
),
  pagamento_rs AS (
    SELECT
      ano,
      mes,
      data,
      sigla_uf,
      id_municipio,
      orgao,
      id_unidade_gestora,
      id_empenho_bd,
      id_empenho,
      numero_empenho,
      id_liquidacao_bd,
      id_liquidacao,
      numero_liquidacao,
      CASE WHEN ddata = 1 OR dorgao = 1 OR dugest = 1 OR dcredor = 1 OR ddocumento = 1 OR (numero_liquidacao = '0' AND valor_final = 0) OR (numero = '0' AND valor_final = 0) THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_pagamento_bd END AS id_pagamento_bd,
      id_pagamento,
      numero,
      nome_credor,
      documento_credor,
      indicador_restos_pagar,
      fonte,
      valor_inicial,
      valor_anulacao,
      valor_ajuste,
      valor_final,
      valor_liquido_recebido
    FROM pagamento1_rs p
    LEFT JOIN dummies d ON p.id_pagamento_bd=d.id_pagamento_bd
),
  pago_sp AS (
   SELECT
     SAFE_CAST (ano_exercicio AS INT64) AS ano,
     SAFE_CAST (mes_referencia AS INT64) AS mes,
     SAFE_CAST (CONCAT(SUBSTRING(dt_emissao_despesa,-4),'-',SUBSTRING(dt_emissao_despesa,-7,2),'-',SUBSTRING(dt_emissao_despesa,1,2)) AS DATE) AS data,
     sigla_uf,
     SAFE_CAST (id_municipio AS STRING) AS id_municipio,
     SAFE_CAST (codigo_orgao AS STRING) AS orgao,
     SAFE_CAST (NULL AS STRING) AS id_unidade_gestora,
     SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_empenho_bd,
     SAFE_CAST (NULL AS STRING) AS id_empenho,
     SAFE_CAST (nr_empenho AS STRING) AS numero_empenho,
     SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', REGEXP_REPLACE(identificador_despesa, '[^0-9]', ''), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_liquidacao_bd,
     SAFE_CAST (NULL AS STRING) AS id_liquidacao,
     SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', REGEXP_REPLACE(identificador_despesa, '[^0-9]', ''), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_pagamento_bd,
     SAFE_CAST (NULL AS STRING) AS numero_liquidacao,
     SAFE_CAST (NULL AS STRING) AS id_pagamento,
     SAFE_CAST (NULL AS STRING) AS numero,
     SAFE_CAST (ds_despesa AS STRING) AS nome_credor,
     SAFE_CAST (REGEXP_REPLACE(identificador_despesa, '[^0-9]', '') AS STRING) AS documento_credor,
     SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
     SAFE_CAST (NULL AS STRING) AS fonte,
     CASE WHEN ds_modalidade_lic = 'CONVITE'                                            THEN '1'
          WHEN ds_modalidade_lic = 'TOMADA DE PREÇOS'                                   THEN '2'
          WHEN ds_modalidade_lic = 'CONCORRÊNCIA'                                       THEN '3'
          WHEN ds_modalidade_lic = 'PREGÃO'                                             THEN '4'
          WHEN ds_modalidade_lic = 'Leilão'                                             THEN '7'
          WHEN ds_modalidade_lic = 'DISPENSA DE LICITAÇÃO'                              THEN '8'
          WHEN ds_modalidade_lic = 'BEC-BOLSA ELETRÔNICA DE COMPRAS'                    THEN '9'
          WHEN ds_modalidade_lic = 'INEXIGÍVEL'                                         THEN '10'
          WHEN ds_modalidade_lic = 'CONCURSO'                                           THEN '11'
          WHEN ds_modalidade_lic = 'RDC'                                                THEN '12'
          WHEN ds_modalidade_lic = 'OUTROS/NÃO APLICÁVEL'                               THEN '99'
     END AS modalidade_licitacao,
     SAFE_CAST (LOWER(historico_despesa) AS STRING) AS descricao,
     SAFE_CAST (NULL AS STRING) AS modalidade,
     SAFE_CAST (funcao AS STRING) AS funcao,
     SAFE_CAST (subfuncao AS STRING) AS subfuncao,
     SAFE_CAST (cd_programa AS STRING) AS programa,
     SAFE_CAST (cd_acao AS STRING) AS acao,
     SAFE_CAST ((LEFT(ds_elemento,8)) AS STRING) AS elemento_despesa,
     SAFE_CAST (REPLACE(vl_despesa, ',', '.') AS FLOAT64) AS valor_inicial
   FROM basedosdados-dev.world_wb_mides_staging.raw_despesa_sp e
   LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_sp m ON m.ds_orgao = e.ds_orgao
   LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_funcao` ON ds_funcao_governo = UPPER(nome_funcao)
   LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_subfuncao` ON ds_subfuncao_governo = UPPER(nome_subfuncao)
   WHERE tp_despesa = 'Valor Pago'
),
  frequencia AS (
     SELECT id_empenho_bd, COUNT (id_empenho_bd) AS frequencia_id
     FROM pago_sp
     GROUP BY 1
     ORDER BY 2 DESC
),
  dorgao AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT orgao)) > 1 THEN 1 ELSE 0 END AS dorgao
    FROM pago_sp
    GROUP BY 1
),
  ddesc AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT IFNULL(descricao,''))) > 1 THEN 1 ELSE 0 END AS ddesc
    FROM pago_sp
    GROUP BY 1
),
  dmod AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT modalidade_licitacao)) > 1 THEN 1 ELSE 0 END AS dmod
    FROM pago_sp
    GROUP BY 1
),
  dfun AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT funcao)) > 1 THEN 1 ELSE 0 END AS dfun
    FROM pago_sp
    GROUP BY 1
),
  dsubf AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT subfuncao)) > 1 THEN 1 ELSE 0 END AS dsubf
    FROM pago_sp
    GROUP BY 1
),
  dprog AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT programa)) > 1 THEN 1 ELSE 0 END AS dprog
    FROM pago_sp
    GROUP BY 1
),
  dacao AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT acao)) > 1 THEN 1 ELSE 0 END AS dacao
    FROM pago_sp
    GROUP BY 1
),
  delem AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT elemento_despesa)) > 1 THEN 1 ELSE 0 END AS delem
    FROM pago_sp
    GROUP BY 1
),
  dcredor AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT nome_credor)) > 1 THEN 1 ELSE 0 END AS dcredor
    FROM pago_sp
    GROUP BY 1
),
  ddocumento AS (
    SELECT
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT documento_credor)) > 1 THEN 1 ELSE 0 END AS ddocumento
    FROM pago_sp
    GROUP BY 1
),
  dummies_sp AS (
    SELECT
      o.id_empenho_bd,
      dorgao,
      dmod,
      ddesc,
      dfun,
      dsubf,
      dprog,
      dacao,
      delem,
      dcredor,
      ddocumento
    FROM dorgao o
    FULL OUTER JOIN dmod m ON o.id_empenho_bd = m.id_empenho_bd
    FULL OUTER JOIN ddesc d ON o.id_empenho_bd = d.id_empenho_bd
    FULL OUTER JOIN dfun f ON o.id_empenho_bd = f.id_empenho_bd
    FULL OUTER JOIN dsubf s ON o.id_empenho_bd = s.id_empenho_bd
    FULL OUTER JOIN dprog p ON o.id_empenho_bd = p.id_empenho_bd
    FULL OUTER JOIN dacao a ON o.id_empenho_bd = a.id_empenho_bd
    FULL OUTER JOIN delem e ON o.id_empenho_bd = e.id_empenho_bd
    FULL OUTER JOIN dcredor c ON o.id_empenho_bd = c.id_empenho_bd
    FULL OUTER JOIN ddocumento dc ON o.id_empenho_bd = dc.id_empenho_bd
),
  frequencia_pg_sp AS (
    SELECT id_pagamento_bd, COUNT(id_pagamento_bd) frequencia_id
    FROM pago_sp
    GROUP BY 1
),
  pagamento_sp AS (
    SELECT
      MIN(ano) AS ano,
      MIN(mes) AS mes,
      MIN(data) AS data,
      sigla_uf,
      id_municipio,
      orgao,
      id_unidade_gestora,
      (CASE WHEN (dorgao = 1 OR dmod = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_empenho_bd END) AS id_empenho_bd,
      id_empenho,
      numero_empenho,
      (CASE WHEN (dorgao = 1 OR dmod = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_liquidacao_bd END) AS id_liquidacao_bd,
      id_liquidacao,
      numero_liquidacao,
      (CASE WHEN (dorgao = 1 OR dmod = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1 OR dcredor = 1 OR ddocumento = 1) OR frequencia_id > 1 THEN (SAFE_CAST (NULL AS STRING)) ELSE p.id_pagamento_bd END) AS id_pagamento_bd,
      id_pagamento,
      numero,
      nome_credor,
      documento_credor,
      indicador_restos_pagar,
      fonte,
      ROUND(SAFE_CAST (SUM(valor_inicial) AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (SUM(valor_inicial) AS FLOAT64),2) AS valor_final,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_liquido_recebido
    FROM pago_sp p
    LEFT JOIN dummies_sp d ON d.id_empenho_bd=p.id_empenho_bd
    LEFT JOIN frequencia_pg_sp f ON f.id_pagamento_bd=p.id_pagamento_bd
    GROUP BY 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)

SELECT
  *
FROM pagamento_mg
UNION ALL (SELECT * FROM pagamento_sp)
UNION ALL (SELECT * FROM pagamento_pe)
UNION ALL (SELECT * FROM pagamento_pr)
UNION ALL (SELECT * FROM pagamento_rs)
UNION ALL (SELECT * FROM pagamento_pb)
UNION ALL (SELECT * FROM pagamento_ce)
)