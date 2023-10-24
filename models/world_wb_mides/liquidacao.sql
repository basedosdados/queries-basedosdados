{{ 
  config(
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
  numero                 STRING,
  nome_responsavel       STRING,
  documento_responsavel  STRING,
  indicador_restos_pagar BOOL,
  valor_inicial          FLOAT64,
  valor_anulacao         FLOAT64,
  valor_ajuste           FLOAT64,
  valor_final            FLOAT64
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
    FROM basedosdados-dev.world_wb_mides_staging.raw_liquidacao_ce l
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_ce m ON l.codigo_municipio = m.codigo_municipio
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
  FROM basedosdados-dev.world_wb_mides_staging.raw_liquidacao_mg AS l
  LEFT JOIN basedosdados-dev.world_wb_mides_staging.raw_rsp_mg AS r ON l.id_rsp=r.id_rsp
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
  FROM basedosdados-dev.world_wb_mides_staging.raw_liquidacao_pb l
  LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pb m ON l.cd_ugestora = SAFE_CAST(m.id_unidade_gestora AS STRING)
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
  FROM basedosdados-dev.world_wb_mides_staging.raw_liquidacao_pr l
  LEFT JOIN basedosdados.br_bd_diretorios_brasil.municipio m ON cdIBGE = id_municipio_6
  LEFT JOIN basedosdados-dev.world_wb_mides_staging.raw_empenho_pr e ON l.idEmpenho = e.idEmpenho
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
    FROM basedosdados-dev.world_wb_mides_staging.raw_liquidacao_pe l
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pe m ON l.ID_UNIDADE_GESTORA = SAFE_CAST(m.ID_UNIDADEGESTORA AS STRING)
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
  FROM `basedosdados-dev.world_wb_mides_staging.raw_despesa_rs` AS c
  LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
  LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
  WHERE tipo_operacao = 'L' AND (SAFE_CAST(vl_liquidacao AS FLOAT64) >= 0)
  GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
),
  estorno_rs AS (
    SELECT 
      SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
      -1*SUM(SAFE_CAST(vl_liquidacao AS FLOAT64)) AS valor_anulacao
    FROM `basedosdados-dev.world_wb_mides_staging.raw_despesa_rs` AS c
    LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
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
   FROM basedosdados-dev.world_wb_mides_staging.raw_despesa_sp e
   LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_sp m ON m.ds_orgao = e.ds_orgao
   LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_funcao` ON ds_funcao_governo = UPPER(nome_funcao)
   LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_subfuncao` ON ds_subfuncao_governo = UPPER(nome_subfuncao)
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
)

SELECT 
  *
FROM liquidacao_mg
UNION ALL (SELECT * FROM liquidacao_sp)
UNION ALL (SELECT * FROM liquidacao_pe)
UNION ALL (SELECT * FROM liquidacao_pr)
UNION ALL (SELECT * FROM liquidacao_rs)
UNION ALL (SELECT * FROM liquidacao_pb)
UNION ALL (SELECT * FROM liquidacao_ce)
)