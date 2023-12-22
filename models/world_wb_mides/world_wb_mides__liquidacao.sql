{{ 
  config(
    alias = 'liquidacao',
    schema='world_wb_mides',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1995,
        "end": 2022,
        "interval": 1}
    },
    cluster_by = ["mes", "sigla_uf"],
    labels = {'tema': 'economia'})
}}
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
  numero,
  nome_responsavel,
  documento_responsavel,
  indicador_restos_pagar,
  valor_inicial,
  valor_anulacao,
  valor_ajuste,
  valor_final
FROM (  
WITH liquidacao_ce AS (
      SELECT
      (SAFE_CAST(EXTRACT(YEAR FROM DATE (data_liquidacao)) AS INT64)) AS ano,
      (SAFE_CAST(EXTRACT(MONTH FROM DATE (data_liquidacao)) AS INT64)) AS mes,
      SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(data_liquidacao)) AS DATE) AS data,
      'CE' AS sigla_uf, 
      SAFE_CAST (geoibgeId AS STRING) AS id_municipio,
      SAFE_CAST (codigo_orgao AS STRING) AS  orgao,
      SAFE_CAST (codigo_unidade AS STRING) AS id_unidade_gestora,
      SAFE_CAST (CONCAT(numero_empenho, ' ', TRIM(codigo_orgao), ' ', TRIM(codigo_unidade), ' ', geoibgeId, ' ', (SUBSTRING(data_emissao_empenho,6,2)), ' ', (SUBSTRING(data_emissao_empenho,3,2))) AS STRING) AS id_empenho_bd,    
      SAFE_CAST (NULL AS STRING) AS  id_empenho,
      SAFE_CAST (numero_empenho AS STRING) AS numero_empenho,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao,
      SAFE_CAST (NULL AS STRING) AS numero,
      SAFE_CAST (nome_responsavel_liquidacao AS STRING) AS nome_responsavel,
      SAFE_CAST (cpf_responsavel_liquidacao_ AS STRING) AS documento_responsavel,
      SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
      ROUND(SAFE_CAST (valor_liquidado AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (valor_liquidado AS FLOAT64),2) AS valor_final,
    FROM basedosdados-staging.world_wb_mides_staging.raw_liquidacao_ce l
    LEFT JOIN basedosdados-staging.world_wb_mides_staging.aux_municipio_ce m ON l.codigo_municipio = m.codigo_municipio
),
  liquidacao_mg AS (
    SELECT
      SAFE_CAST (ano AS INT64) AS ano,
      SAFE_CAST (mes AS INT64) AS mes,
      SAFE_CAST (data AS DATE) AS data,
      'MG' AS sigla_uf,
      SAFE_CAST (l.id_municipio AS STRING) AS id_municipio,
      SAFE_CAST (l.orgao AS STRING) AS orgao,
      SAFE_CAST (l.id_unidade_gestora AS STRING) AS id_unidade_gestora,
      SAFE_CAST ((CASE 
        WHEN id_empenho != '-1' THEN CONCAT(id_empenho, ' ', l.orgao, ' ', l.id_municipio, ' ', (RIGHT(ano,2)))
        WHEN id_empenho = '-1'  THEN CONCAT(id_empenho_origem, ' ', r.orgao, ' ', r.id_municipio, ' ', (RIGHT(num_ano_emp_origem,2)))
        END) AS STRING) AS id_empenho_bd,
      SAFE_CAST ((CASE 
        WHEN id_empenho = '-1' THEN REPLACE (id_empenho, '-1', id_empenho_origem) END) AS STRING) AS id_empenho,
      SAFE_CAST (numero_empenho AS STRING) AS numero_empenho,
      SAFE_CAST (CONCAT(id_liquidacao, ' ', l.orgao, ' ', l.id_municipio, ' ', (RIGHT(ano,2))) AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (id_liquidacao AS STRING) AS id_liquidacao,
      SAFE_CAST (numero_liquidacao AS STRING) AS numero,
      SAFE_CAST (nome_responsavel AS STRING) AS nome_responsavel,
      SAFE_CAST (documento_responsavel AS STRING) AS documento_responsavel,
      SAFE_CAST ((CASE WHEN l.id_rsp != '-1' THEN 1 ELSE 0 END) AS BOOL) AS indicador_restos_pagar,
      ROUND(SAFE_CAST (valor_liquidacao_original AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (valor_anulado AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (valor_liquidacao_original AS FLOAT64) - IFNULL(SAFE_CAST (valor_anulado AS FLOAT64),0),2) AS valor_final
  FROM basedosdados-staging.world_wb_mides_staging.raw_liquidacao_mg AS l
  LEFT JOIN basedosdados-staging.world_wb_mides_staging.raw_rsp_mg AS r ON l.id_rsp=r.id_rsp
),
  liquidacao_pb AS (
    SELECT
    SAFE_CAST (dt_Ano AS INT64) AS ano,
    (SAFE_CAST(SUBSTRING(dt_Liquidacao,-7,2) AS INT64)) AS mes,
    SAFE_CAST (CONCAT(SUBSTRING(dt_Liquidacao,-4),'-',SUBSTRING(dt_Liquidacao,-7,2),'-',SUBSTRING(dt_Liquidacao,1,2)) AS DATE) AS data,
    'PB' AS sigla_uf, 
    SAFE_CAST (id_municipio AS STRING) AS id_municipio,
    SAFE_CAST (NULL AS STRING) AS  orgao,
    SAFE_CAST (l.cd_UGestora AS STRING) AS id_unidade_gestora,
    SAFE_CAST (CONCAT(nu_Empenho, ' ', l.cd_ugestora, ' ', m.id_municipio, ' ', (RIGHT(dt_Ano,2))) AS STRING) AS id_empenho_bd,
    SAFE_CAST (NULL AS STRING) AS  id_empenho,
    SAFE_CAST (nu_Empenho AS STRING) AS numero_empenho,
    SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
    SAFE_CAST (NULL AS STRING) AS id_liquidacao,
    SAFE_CAST (nu_Liquidacao AS STRING) AS numero,
    SAFE_CAST (NULL AS STRING) AS nome_responsavel,
    SAFE_CAST (NULL AS STRING) AS documento_responsavel,
    SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
    ROUND(SAFE_CAST (vl_Liquidacao AS FLOAT64),2) AS valor_inicial,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SAFE_CAST (vl_Liquidacao AS FLOAT64),2) AS valor_final,
  FROM basedosdados-staging.world_wb_mides_staging.raw_liquidacao_pb l
  LEFT JOIN basedosdados-staging.world_wb_mides_staging.aux_municipio_pb m ON l.cd_ugestora = SAFE_CAST(m.id_unidade_gestora AS STRING)
),
  liquidacao_pr AS (
    SELECT
    SAFE_CAST (nrAnoLiquidacao AS INT64) AS ano,
    (SAFE_CAST(EXTRACT(MONTH FROM DATE (dtLiquidacao)) AS INT64)) AS mes,
    SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(dtLiquidacao)) AS DATE) AS data,
    'PR' AS sigla_uf, 
    SAFE_CAST (id_municipio AS STRING) AS id_municipio,
    SAFE_CAST (cdOrgao AS STRING) AS orgao,
    SAFE_CAST (cdUnidade AS STRING) AS id_unidade_gestora,
    SAFE_CAST (CONCAT(l.idEmpenho, ' ', m.id_municipio) AS STRING) AS id_empenho_bd,
    SAFE_CAST (l.idEmpenho AS STRING) AS id_empenho,
    SAFE_CAST (nrEmpenho AS STRING) AS numero_empenho,
    SAFE_CAST (CONCAT(l.idLiquidacao,' ', m.id_municipio) AS STRING) AS id_liquidacao_bd,
    SAFE_CAST (idLiquidacao AS STRING) AS id_liquidacao,
    SAFE_CAST (nrLiquidacao AS STRING) AS numero,
    SAFE_CAST (nmLiquidante AS STRING) AS nome_responsavel,
    SAFE_CAST (nrDocLiquidante AS STRING) AS documento_responsavel,
    SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
    ROUND(SAFE_CAST (vlLiquidacaoBruto AS FLOAT64),2) AS valor_inicial,
    ROUND(SAFE_CAST (vlLiquidacaoEstornado AS FLOAT64),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SAFE_CAST (vlLiquidacaoLiquido AS FLOAT64),2) AS valor_final,
  FROM basedosdados-staging.world_wb_mides_staging.raw_liquidacao_pr l
  LEFT JOIN basedosdados.br_bd_diretorios_brasil.municipio m ON cdIBGE = id_municipio_6
  LEFT JOIN basedosdados-staging.world_wb_mides_staging.raw_empenho_pr e ON l.idEmpenho = e.idEmpenho
),
  liquidacao_pe AS (
    SELECT
      SAFE_CAST (l.ANOREFERENCIA AS INT64) AS ano,
      (SAFE_CAST(EXTRACT(MONTH FROM DATE(DATA)) AS INT64)) AS mes,
      SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(DATA)) AS DATE) AS data,
      'PE' AS sigla_uf, 
      SAFE_CAST (CODIGOIBGE AS STRING) AS id_municipio,
      SAFE_CAST (NULL AS STRING) orgao,
      SAFE_CAST (ID_UNIDADEGESTORA AS STRING) AS id_unidade_gestora,
      SAFE_CAST (NULL AS STRING) AS id_empenho_bd,
      SAFE_CAST (TRIM(IDEMPENHO) AS STRING) AS id_empenho,
      SAFE_CAST (l.NUMEROEMPENHO AS STRING) AS numero_empenho,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_liquidacao,
      SAFE_CAST (NULL AS STRING) AS numero,
      SAFE_CAST (NULL AS STRING) AS nome_responsavel,
      SAFE_CAST (NULL AS STRING) AS documento_responsavel,
      SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
      ROUND(SAFE_CAST (VALOR AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (VALOR AS FLOAT64),2) AS valor_final,
    FROM basedosdados-staging.world_wb_mides_staging.raw_liquidacao_pe l
    LEFT JOIN basedosdados-staging.world_wb_mides_staging.aux_municipio_pe m ON l.ID_UNIDADE_GESTORA = SAFE_CAST(m.ID_UNIDADEGESTORA AS STRING)
),
  liquidado_rs AS (
  SELECT
    MIN(ano_recebimento) AS ano_recebimento,
    SAFE_CAST(ano_operacao AS INT64) AS ano,
    SAFE_CAST(EXTRACT(MONTH FROM DATE(dt_operacao)) AS INT64) AS mes,
    SAFE_CAST(CONCAT(SUBSTRING(dt_operacao,1,4), '-', SUBSTRING(dt_operacao,6,2),  '-', SUBSTRING(dt_operacao,9,2)) AS DATE) AS data,
    'RS' AS sigla_uf,
    SAFE_CAST(a.id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(c.cd_orgao AS STRING) AS orgao,
    SAFE_CAST(cd_orgao_orcamentario AS STRING) AS id_unidade_gestora,
    SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
    SAFE_CAST(NULL AS STRING) AS id_empenho,
    SAFE_CAST(nr_empenho AS STRING) AS numero_empenho,
    SAFE_CAST(CONCAT(nr_liquidacao, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_liquidacao_bd,
    SAFE_CAST (NULL AS STRING) AS id_liquidacao,
    SAFE_CAST (nr_liquidacao AS STRING) AS numero,
    SAFE_CAST (NULL AS STRING) AS nome_responsavel,
    SAFE_CAST (NULL AS STRING) AS documento_responsavel,
    SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
    SAFE_CAST(vl_liquidacao AS FLOAT64) AS valor_inicial
  FROM `basedosdados-staging.world_wb_mides_staging.raw_despesa_rs` AS c
  LEFT JOIN `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
  LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
  WHERE tipo_operacao = 'L' AND (SAFE_CAST(vl_liquidacao AS FLOAT64) >= 0)
  GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),
  estorno_rs AS (
    SELECT 
      SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
      -1*SUM(SAFE_CAST(vl_liquidacao AS FLOAT64)) AS valor_anulacao
    FROM `basedosdados-staging.world_wb_mides_staging.raw_despesa_rs` AS c
    LEFT JOIN `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
    LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
    WHERE tipo_operacao = 'L' AND (SAFE_CAST(vl_liquidacao AS FLOAT64) < 0)
    GROUP BY 1   
),
  frequencia_rs AS (
    SELECT 
      id_empenho_bd, COUNT(id_empenho_bd) AS frequencia_id
    FROM liquidado_rs
    GROUP BY 1
  ),
    liquidacao1_rs AS (
      SELECT 
        ano,
        mes,
        data,
        sigla_uf,
        id_municipio,
        orgao,
        id_unidade_gestora,
        l.id_empenho_bd,
        id_empenho,
        numero_empenho,
        id_liquidacao_bd,
        id_liquidacao,
        numero,
        nome_responsavel,
        documento_responsavel,
        indicador_restos_pagar,
        SUM(valor_inicial) AS valor_inicial,
        SUM(valor_anulacao/frequencia_id) AS valor_anulacao,
        ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
        SUM(valor_inicial - IFNULL((valor_anulacao/frequencia_id), 0)) AS valor_final
      FROM liquidado_rs l
      LEFT JOIN estorno_rs e ON l.id_empenho_bd=e.id_empenho_bd
      LEFT JOIN frequencia_rs f ON l.id_empenho_bd=f.id_empenho_bd
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),
  data_rs AS (
    SELECT 
      id_liquidacao_bd,
      CASE WHEN (COUNT (DISTINCT data)) > 1 THEN 1 ELSE 0 END AS ddata
    FROM liquidacao1_rs
    GROUP BY 1
),
  liquidacao_rs AS (
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
        CASE WHEN ddata = 1 THEN (SAFE_CAST (NULL AS STRING)) ELSE l.id_liquidacao_bd END AS id_liquidacao_bd,
        id_liquidacao,
        numero,
        nome_responsavel,
        documento_responsavel,
        indicador_restos_pagar,
        ROUND(valor_inicial,2),
        ROUND(IFNULL(valor_anulacao,0),2),
        valor_ajuste,
        ROUND(valor_final,2)
    FROM liquidacao1_rs l
    LEFT JOIN data_rs d ON l.id_liquidacao_bd=d.id_liquidacao_bd
),
  liquidado_sp AS (
   SELECT
     SAFE_CAST (ano_exercicio AS INT64) AS ano,
     SAFE_CAST (mes_referencia AS INT64) AS mes,
     SAFE_CAST (CONCAT(SUBSTRING(dt_emissao_despesa,-4),'-',SUBSTRING(dt_emissao_despesa,-7,2),'-',SUBSTRING(dt_emissao_despesa,1,2)) AS DATE) AS data,
     'SP' AS sigla_uf,
     SAFE_CAST (id_municipio AS STRING) AS id_municipio,
     SAFE_CAST (codigo_orgao AS STRING) AS orgao,
     SAFE_CAST (NULL AS STRING) AS id_unidade_gestora,
     SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_empenho_bd,
     SAFE_CAST (NULL AS STRING) AS id_empenho,
     SAFE_CAST (nr_empenho AS STRING) AS numero_empenho,
     SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', REGEXP_REPLACE(identificador_despesa, '[^0-9]', ''), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_liquidacao_bd,
     SAFE_CAST (NULL AS STRING) AS id_liquidacao,
     SAFE_CAST (NULL AS STRING) AS numero,
     SAFE_CAST (NULL AS STRING) AS nome_responsavel,
     SAFE_CAST (NULL AS STRING) AS documento_responsavel,
     SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
     SAFE_CAST (nr_empenho AS STRING) AS numero,
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
   FROM basedosdados-staging.world_wb_mides_staging.raw_despesa_sp e
   LEFT JOIN basedosdados-staging.world_wb_mides_staging.aux_municipio_sp m ON m.ds_orgao = e.ds_orgao
   LEFT JOIN `basedosdados-staging.world_wb_mides_staging.aux_funcao` ON ds_funcao_governo = UPPER(nome_funcao)
   LEFT JOIN `basedosdados-staging.world_wb_mides_staging.aux_subfuncao` ON ds_subfuncao_governo = UPPER(nome_subfuncao)
   WHERE tp_despesa = 'Valor Liquidado'
),
  frequencia AS (
     SELECT id_empenho_bd, COUNT (id_empenho_bd) AS frequencia_id
     FROM liquidado_sp
     GROUP BY 1
     ORDER BY 2 DESC
),
  dorgao AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT orgao)) > 1 THEN 1 ELSE 0 END AS dorgao
    FROM liquidado_sp
    GROUP BY 1
),
  ddesc AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT IFNULL(descricao,''))) > 1 THEN 1 ELSE 0 END AS ddesc
    FROM liquidado_sp
    GROUP BY 1
),
  dmod AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT modalidade_licitacao)) > 1 THEN 1 ELSE 0 END AS dmod
    FROM liquidado_sp
    GROUP BY 1
),
  dfun AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT funcao)) > 1 THEN 1 ELSE 0 END AS dfun
    FROM liquidado_sp
    GROUP BY 1
),
  dsubf AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT subfuncao)) > 1 THEN 1 ELSE 0 END AS dsubf
    FROM liquidado_sp
    GROUP BY 1
),
  dprog AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT programa)) > 1 THEN 1 ELSE 0 END AS dprog
    FROM liquidado_sp
    GROUP BY 1
),
  dacao AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT acao)) > 1 THEN 1 ELSE 0 END AS dacao
    FROM liquidado_sp
    GROUP BY 1
),
  delem AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT elemento_despesa)) > 1 THEN 1 ELSE 0 END AS delem
    FROM liquidado_sp
    GROUP BY 1
),
  dummies AS (
    SELECT 
      o.id_empenho_bd,
      dorgao,
      dmod,
      ddesc,
      dfun,
      dsubf,
      dprog,
      dacao,
      delem  
    FROM dorgao o
    FULL OUTER JOIN dmod m ON o.id_empenho_bd = m.id_empenho_bd
    FULL OUTER JOIN ddesc d ON o.id_empenho_bd = d.id_empenho_bd
    FULL OUTER JOIN dfun f ON o.id_empenho_bd = f.id_empenho_bd
    FULL OUTER JOIN dsubf s ON o.id_empenho_bd = s.id_empenho_bd
    FULL OUTER JOIN dprog p ON o.id_empenho_bd = p.id_empenho_bd
    FULL OUTER JOIN dacao a ON o.id_empenho_bd = a.id_empenho_bd
    FULL OUTER JOIN delem e ON o.id_empenho_bd = e.id_empenho_bd
),
  liquidacao_sp AS (
  SELECT
    MIN(ano) AS ano,
    MIN(mes) AS mes,
    MIN(data) AS data,
    sigla_uf,
    id_municipio,
    orgao,
    id_unidade_gestora,
    (CASE WHEN (dorgao = 1 OR dmod = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE l.id_empenho_bd END) AS id_empenho_bd, 
    id_empenho, 
    numero_empenho,
    id_liquidacao_bd,
    id_liquidacao,
    SAFE_CAST(NULL AS STRING) AS numero,
    nome_responsavel,
    documento_responsavel,
    indicador_restos_pagar,
    ROUND(SUM(valor_inicial),2) AS valor_inicial,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SUM(valor_inicial),2) AS valor_final
  FROM liquidado_sp l
  LEFT JOIN dummies d ON d.id_empenho_bd=l.id_empenho_bd
  GROUP BY 4,5,6,7,8,9,10,11,12,13,14,15,16
),
  liquidacao_municipio_sp AS (
  SELECT
    (SAFE_CAST(exercicio AS INT64)) AS ano,
    (SAFE_CAST(EXTRACT(MONTH FROM DATE (data_empenho)) AS INT64)) AS mes,
    SAFE_CAST (data_empenho AS DATE) AS data,
    'SP' AS sigla_uf,
    '3550308' AS  id_municipio,
    SAFE_CAST (codigo_orgao AS STRING) AS  orgao,
    SAFE_CAST (codigo_unidade AS STRING) AS id_unidade_gestora,
    SAFE_CAST (CONCAT(nr_empenho, ' ', TRIM(codigo_orgao), ' ', TRIM(codigo_unidade), ' ', '3550308', ' ', (RIGHT(exercicio,2))) AS STRING) AS id_empenho_bd,    
    SAFE_CAST (id_empenho AS STRING) AS id_empenho,
    SAFE_CAST (nr_empenho AS STRING) AS numero_empenho,
    SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
    SAFE_CAST (NULL AS STRING) AS id_liquidacao,
    SAFE_CAST (NULL AS STRING) AS numero,
    SAFE_CAST (NULL AS STRING) AS nome_responsavel,
    SAFE_CAST (NULL AS STRING) AS documento_responsavel,
    SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
    ROUND(SAFE_CAST (liquidado AS FLOAT64),2) AS valor_inicial,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SAFE_CAST (liquidado AS FLOAT64),2) AS valor_final
  FROM `basedosdados-staging.world_wb_mides_staging.raw_despesa_sp_municipio` 
),
  liquidado_municipio_rj_v1 AS (
  SELECT
   SAFE_CAST(exercicio_empenho AS INT64) AS ano,
   SAFE_CAST(NULL AS INT64) AS mes,
   SAFE_CAST (NULL AS DATE) AS data,
   'RJ' AS sigla_uf,
   '3304557' AS id_municipio,
   SAFE_CAST (orgao_programa_trabalho AS STRING) AS orgao,
   SAFE_CAST (unidade_programa_trabalho AS STRING) AS id_unidade_gestora,
   SAFE_CAST (CONCAT(nr_empenho, ' ', TRIM(orgao_programa_trabalho), ' ', TRIM(unidade_programa_trabalho), ' ', '3304557', ' ', (RIGHT(exercicio_empenho,2))) AS STRING) AS id_empenho_bd,   
   SAFE_CAST (NULL AS STRING) AS id_empenho,
   SAFE_CAST (nr_empenho AS STRING) AS numero_empenho,
   SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
   SAFE_CAST (NULL AS STRING) AS id_liquidacao,
   SAFE_CAST (NULL AS STRING) AS numero,
   SAFE_CAST (NULL AS STRING) AS nome_responsavel,
   SAFE_CAST (NULL AS STRING) AS documento_responsavel,
   SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
   ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_inicial,
   ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
   ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
   ROUND(SAFE_CAST (valor_liquidado AS FLOAT64),2) AS valor_final
 FROM `basedosdados-staging.world_wb_mides_staging.raw_despesa_rj_municipio`
),
 frequencia_rj_v1 AS (
   SELECT id_empenho_bd, COUNT(id_empenho_bd) AS frequencia_id
   FROM liquidado_municipio_rj_v1
   GROUP BY 1
   ORDER BY 2 DESC
),
 liquidacao_municipio_rj_v1 AS (
   SELECT
     l.ano,
     l.mes,
     l.data,
     l.sigla_uf,
     l.id_municipio,
     l.orgao,
     l.id_unidade_gestora,
     (CASE WHEN frequencia_id > 1 THEN (SAFE_CAST (NULL AS STRING)) ELSE l.id_empenho_bd END) AS id_empenho_bd,
     l.id_empenho,
     l.numero_empenho,
     l.id_liquidacao_bd,
     l.id_liquidacao,
     l.numero,
     l.nome_responsavel,
     l.documento_responsavel,
     l.indicador_restos_pagar,
     l.valor_inicial,
     l.valor_anulacao,
     l.valor_ajuste,
     l.valor_final
   FROM liquidado_municipio_rj_v1 l
   LEFT JOIN frequencia_rj_v1 f ON l.id_empenho_bd = f.id_empenho_bd
),
 liquidado_municipio_rj_v2 AS (
   SELECT
     (SAFE_CAST(Exercicio AS INT64)) AS ano,
     (SAFE_CAST(EXTRACT(MONTH FROM DATE (Data)) AS INT64)) AS mes,
     SAFE_CAST (Data AS DATE) AS data,
     'RJ' AS sigla_uf,
     '3304557' AS  id_municipio,
     SAFE_CAST (UG AS STRING) AS  orgao,
     SAFE_CAST (UO AS STRING) AS id_unidade_gestora,
     SAFE_CAST (CONCAT(LEFT(EmpenhoExercicio, LENGTH(EmpenhoExercicio) - 5), ' ', TRIM(UO), ' ', TRIM(UG), ' ', '3304557', ' ', (RIGHT(Exercicio,2))) AS STRING) AS id_empenho_bd,   
     SAFE_CAST (NULL AS STRING) AS id_empenho,
     SAFE_CAST (EmpenhoExercicio AS STRING) AS numero_empenho,
     SAFE_CAST (CONCAT(Liquidacao, ' ', LEFT(EmpenhoExercicio, LENGTH(EmpenhoExercicio) - 5), ' ', TRIM(UO), ' ', TRIM(UG), ' ', '3304557', ' ', (RIGHT(Exercicio,2))) AS STRING) AS id_liquidacao_bd,
     SAFE_CAST (NULL AS STRING) AS id_liquidacao,
     SAFE_CAST (Liquidacao AS STRING) AS numero,
     SAFE_CAST (NULL AS STRING) AS nome_responsavel,
     SAFE_CAST (NULL AS STRING) AS documento_responsavel,
     SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
     ROUND(SAFE_CAST (Valor AS FLOAT64),2) AS valor_inicial
   FROM `basedosdados-staging.world_wb_mides_staging.raw_despesa_ato_rj_municipio`
   WHERE TipoAto = 'LIQUIDACAO'
   ),
 anulacao_municipio_rj_v2 AS (
   SELECT
     SAFE_CAST (TipoAto AS STRING) AS TipoAto,
     SAFE_CAST (CONCAT(LEFT(EmpenhoExercicio, LENGTH(EmpenhoExercicio) - 5), ' ', TRIM(UO), ' ', TRIM(UG), ' ', '3304557', ' ', (RIGHT(Exercicio,2))) AS STRING) AS id_empenho_bd,
     SUM(SAFE_CAST (Valor AS FLOAT64)) AS valor_anulacao,
   FROM `basedosdados-staging.world_wb_mides_staging.raw_despesa_ato_rj_municipio`
   WHERE TipoAto IN ('CANCELAMENTO LIQUIDACAO', 'Cancelamento de liquidação de RPN', 'CANCELAMENTO DE RPN')
   GROUP BY 1,2
),
 frequencia_rj_v2 AS (
   SELECT
     id_empenho_bd, COUNT (1) AS frequencia
   FROM anulacao_municipio_rj_v2
   GROUP BY 1
),
 liquidacao_municipio_rj_v2 AS (
   SELECT
     l.ano,
     l.mes,
     l.data,
     l.sigla_uf,
     l.id_municipio,
     l.orgao,
     l.id_unidade_gestora,
     l.id_empenho_bd,
     l.id_empenho,
     l.numero_empenho,
     l.id_liquidacao_bd,
     l.id_liquidacao,
     l.numero,
     l.nome_responsavel,
     l.documento_responsavel,
     CASE WHEN TipoAto = 'Cancelamento de liquidação de RPN' THEN true
          WHEN TipoAto = 'CANCELAMENTO DE RPN'               THEN true
          ELSE false
     END AS indicador_restos_pagar,
     ROUND (SAFE_CAST(l.valor_inicial AS FLOAT64), 2) AS valor_inicial,
     ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
     ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
     ROUND (SAFE_CAST(l.valor_inicial AS FLOAT64), 2) AS valor_final
   FROM liquidado_municipio_rj_v2 l
   LEFT JOIN anulacao_municipio_rj_v2 a ON l.id_empenho_bd = a.id_empenho_bd
   LEFT JOIN frequencia_rj_v2 f ON l.id_empenho_bd = f.id_empenho_bd
),
 liquidacao_rj AS (
   SELECT
     (SAFE_CAST(ano AS INT64)) AS ano,
     (SAFE_CAST(EXTRACT(MONTH FROM DATE (data)) AS INT64)) AS mes,
     SAFE_CAST (data AS DATE) AS data,
     'RJ' AS sigla_uf,
     SAFE_CAST (id_municipio AS STRING) AS  id_municipio,
     SAFE_CAST (id_orgao AS STRING) AS  orgao,
     SAFE_CAST (unidade_administrativa AS STRING) AS id_unidade_gestora,
     SAFE_CAST (CONCAT(numero_empenho, ' ', id_orgao, ' ', id_municipio, ' ', (RIGHT(ano,2))) AS STRING) AS id_empenho_bd,   
     SAFE_CAST (NULL AS STRING) AS id_empenho,
     SAFE_CAST (numero_empenho AS STRING) AS numero_empenho,
     SAFE_CAST (NULL AS STRING) AS id_liquidacao_bd,
     SAFE_CAST (NULL AS STRING) AS id_liquidacao,
     SAFE_CAST (NULL AS STRING) AS numero,
     SAFE_CAST (NULL AS STRING) AS nome_responsavel,
     SAFE_CAST (NULL AS STRING) AS documento_responsavel,
     SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
     ROUND(SAFE_CAST (valor AS FLOAT64),2) AS valor_inicial,
     ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
     ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
     ROUND(SAFE_CAST (valor AS FLOAT64),2) AS valor_final
   FROM `basedosdados-staging.world_wb_mides_staging.raw_liquidacao_rj`
   WHERE numero_empenho IS NOT NULL
),
  liquidacao_df AS (
    SELECT
     (SAFE_CAST(exercicio AS INT64)) AS ano,
     (SAFE_CAST(EXTRACT(MONTH FROM DATE (emissao)) AS INT64)) AS mes,
     SAFE_CAST (emissao AS DATE) AS data,
     'DF' AS sigla_uf,
     '5300108' AS  id_municipio,
      SAFE_CAST (codigo_ug AS STRING) AS  orgao,
      SAFE_CAST (codigo_gestao AS STRING) AS id_unidade_gestora,
     SAFE_CAST (CONCAT(RIGHT(nota_empenho, LENGTH(nota_empenho) - 6), ' ', codigo_ug, ' ', codigo_gestao, ' ', '5300108', ' ', (RIGHT(exercicio,2))) AS STRING) AS id_empenho_bd,   
     SAFE_CAST (NULL AS STRING) AS id_empenho,
     SAFE_CAST (nota_empenho AS STRING) AS numero_empenho,
     CASE WHEN LENGTH(nota_lancamento) = 11 THEN SAFE_CAST (CONCAT(RIGHT(nota_lancamento, LENGTH(nota_lancamento) - 6), ' ', codigo_ug, ' ', codigo_gestao, ' ', '5300108', ' ', (RIGHT(exercicio,2))) AS STRING) END AS id_liquidacao_bd,
     SAFE_CAST (NULL AS STRING) AS id_liquidacao,
     SAFE_CAST (nota_lancamento AS STRING) AS numero,
     SAFE_CAST (credor AS STRING) AS nome_responsavel,
     SAFE_CAST (cnpj_cpf_credor AS STRING) AS documento_responsavel,
     SAFE_CAST (NULL AS BOOL) AS indicador_restos_pagar,
     ROUND(SAFE_CAST (valor AS FLOAT64),2) AS valor_inicial,
     ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
     ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
     ROUND(SAFE_CAST (valor AS FLOAT64),2) AS valor_final
   FROM `basedosdados-staging.world_wb_mides_staging.raw_liquidacao_df`
)


SELECT 
  *
FROM liquidacao_mg
UNION ALL (SELECT * FROM liquidacao_sp)
UNION ALL (SELECT * FROM liquidacao_municipio_sp)
UNION ALL (SELECT * FROM liquidacao_pe)
UNION ALL (SELECT * FROM liquidacao_pr)
UNION ALL (SELECT * FROM liquidacao_rs)
UNION ALL (SELECT * FROM liquidacao_pb)
UNION ALL (SELECT * FROM liquidacao_ce)
UNION ALL (SELECT * FROM liquidacao_municipio_rj_v1)
UNION ALL (SELECT * FROM liquidacao_municipio_rj_v2)
UNION ALL (SELECT * FROM liquidacao_rj)
UNION ALL (SELECT * FROM liquidacao_df)
)