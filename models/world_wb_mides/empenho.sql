{{ 
  config(
    schema='world_wb_mides',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1994,
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
  id_licitacao_bd        STRING,
  id_licitacao           STRING,
  modalidade_licitacao   STRING,
  id_empenho_bd          STRING,
  id_empenho             STRING,
  numero                 STRING,
  descricao              STRING,
  modalidade             STRING,
  funcao                 STRING,
  subfuncao              STRING,
  programa               STRING,
  acao                   STRING,
  elemento_despesa       STRING,
  valor_inicial          FLOAT64,
  valor_reforco          FLOAT64,
  valor_anulacao         FLOAT64,
  valor_ajuste           FLOAT64,
  valor_final            FLOAT64
FROM (
WITH empenhado_ce AS (
  SELECT
    (SAFE_CAST(EXTRACT(YEAR FROM DATE (data_emissao_empenho)) AS INT64)) AS ano,
    (SAFE_CAST(EXTRACT(MONTH FROM DATE (data_emissao_empenho)) AS INT64)) AS mes,
    SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(data_emissao_empenho)) AS DATE) AS data,
    'CE' AS sigla_uf,
    SAFE_CAST (geoibgeId AS STRING) AS  id_municipio,
    SAFE_CAST (codigo_orgao AS STRING) AS  orgao,
    SAFE_CAST (TRIM(codigo_unidade) AS STRING) AS id_unidade_gestora,
    SAFE_CAST (NULL AS STRING) AS id_licitacao_bd,
    SAFE_CAST (numero_licitacao AS STRING) AS id_licitacao,
    SAFE_CAST (tipo_processo_licitatorio AS STRING) AS modalidade_licitacao,
    SAFE_CAST (CONCAT(numero_empenho, ' ', TRIM(codigo_orgao), ' ', TRIM(codigo_unidade), ' ', geoibgeId, ' ', (SUBSTRING(data_emissao_empenho,6,2)), ' ', (SUBSTRING(data_emissao_empenho,3,2))) AS STRING) AS id_empenho_bd,    
    SAFE_CAST (NULL AS STRING) AS id_empenho,
    SAFE_CAST (numero_empenho AS STRING) AS numero,
    SAFE_CAST (LOWER (descricao_empenho) AS STRING) AS descricao,
    SAFE_CAST (modalidade_empenho AS STRING) AS modalidade,
    SAFE_CAST (SAFE_CAST (codigo_funcao AS INT64) AS STRING) AS funcao,
    SAFE_CAST (SAFE_CAST (codigo_subfuncao AS INT64) AS STRING) AS subfuncao,
    SAFE_CAST (SAFE_CAST (codigo_programa AS INT64) AS STRING) AS programa,
    SAFE_CAST (SAFE_CAST (codigo_projeto_atividade AS INT64) AS STRING) AS acao,
    SAFE_CAST (SAFE_CAST (codigo_elemento_despesa AS INT64) AS STRING) AS modalidade_despesa,
    ROUND(SAFE_CAST (valor_empenhado AS FLOAT64),2) AS valor_inicial,
  FROM basedosdados-dev.world_wb_mides_staging.raw_empenho_ce e
),
  anulacao_ce AS (
    SELECT 
      SAFE_CAST (CONCAT(numero_empenho, ' ', TRIM(codigo_orgao), ' ', TRIM(codigo_unidade), ' ', geoibgeId, ' ', (SUBSTRING(data_emissao_empenho,6,2)), ' ', (SUBSTRING(data_emissao_empenho,3,2))) AS STRING) AS id_empenho_bd, 
      ROUND(SUM(SAFE_CAST (valor_anulacao AS FLOAT64)),2) AS valor_anulacao   
    FROM basedosdados-dev.world_wb_mides_staging.raw_anulacao_ce
    GROUP BY 1
),
  frequencia_ce AS (
    SELECT id_empenho_bd, COUNT(id_empenho_bd) AS frequencia_id
    FROM empenhado_ce
    GROUP BY 1
    ORDER BY 2 DESC
),
  empenho_ce AS (
    SELECT 
      e.ano,
      e.mes,
      e.data,
      e.sigla_uf,
      e.id_municipio,
      e.orgao,
      e.id_unidade_gestora,
      e.id_licitacao_bd,
      e.id_licitacao,
      e.modalidade_licitacao,
      (CASE WHEN frequencia_id > 1 THEN (SAFE_CAST (NULL AS STRING)) ELSE e.id_empenho_bd END) AS id_empenho_bd,
      e.id_empenho,
      e.numero,
      e.descricao,
      e.modalidade,
      e.funcao,
      e.subfuncao,
      e.programa,
      e.acao,
      e.modalidade_despesa,
      ROUND(e.valor_inicial,2),
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS  valor_reforco,
      ROUND(a.valor_anulacao,2),
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(IFNULL(e.valor_inicial,0) - IFNULL(a.valor_anulacao, 0),2) AS valor_final
    FROM empenhado_ce e
    LEFT JOIN frequencia_ce f ON e.id_empenho_bd = f.id_empenho_bd 
    FULL OUTER JOIN anulacao_ce a ON a.id_empenho_bd = e.id_empenho_bd 
),
empenhado_mg AS (
  SELECT
    SAFE_CAST (ano AS INT64) AS ano,
    SAFE_CAST (mes AS INT64) AS mes,
    SAFE_CAST (data AS DATE) AS data,
    'MG' AS sigla_uf,
    SAFE_CAST (id_municipio AS STRING) AS id_municipio,
    SAFE_CAST (TRIM(orgao) AS STRING) AS orgao,
    SAFE_CAST (id_unidade_gestora AS STRING) AS id_unidade_gestora,
    SAFE_CAST (NULL AS STRING) AS id_licitacao_bd,
    SAFE_CAST (id_licitacao AS STRING) AS id_licitacao,
    SAFE_CAST (NULL AS STRING) AS modalidade_licitacao,
    SAFE_CAST (CONCAT(id_empenho, ' ', orgao, ' ', id_municipio, ' ', (RIGHT(ano,2))) AS STRING) AS id_empenho_bd,
    SAFE_CAST (id_empenho AS STRING) AS id_empenho,
    SAFE_CAST (numero_empenho AS STRING) AS numero,
    SAFE_CAST (LOWER (descricao) AS STRING) AS descricao,
    SAFE_CAST (SUBSTRING (dsc_modalidade, 5,1) AS STRING) AS modalidade,
    SAFE_CAST (CAST(LEFT(dsc_funcao, 2) AS INT64) AS STRING) AS funcao,
    SAFE_CAST (CAST(LEFT(dsc_subfuncao, 3) AS INT64) AS STRING) AS subfuncao,
    SAFE_CAST (CAST(LEFT(dsc_programa, 4) AS INT64) AS STRING) AS programa,
    SAFE_CAST (CAST(LEFT(dsc_acao, 4) AS INT64) AS STRING) AS acao,
    SAFE_CAST (REPLACE(LEFT(elemento_despesa, 12), '.', '') AS STRING) AS elemento_despesa,
    ROUND(SAFE_CAST (valor_empenho_original AS FLOAT64),2) AS valor_inicial,
    ROUND(SAFE_CAST (IFNULL(valor_reforco,0) AS FLOAT64),2) AS valor_reforco,
    ROUND(SAFE_CAST (IFNULL(valor_anulacao,0) AS FLOAT64),2) AS valor_anulacao,
    ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    ROUND(SAFE_CAST (valor_empenho_original AS FLOAT64) + SAFE_CAST (IFNULL(valor_reforco,0) AS FLOAT64) - SAFE_CAST (IFNULL(valor_anulacao,0) AS FLOAT64),2) AS valor_final
  FROM basedosdados-dev.world_wb_mides_staging.raw_empenho_mg
),
  dlic AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT id_licitacao)) > 1 THEN 1 ELSE 0 END AS dlic
    FROM empenhado_mg
    GROUP BY 1
),
  empenho_mg AS (
    SELECT DISTINCT
      e.ano,
      e.mes,
      e.data,
      e.sigla_uf,
      e.id_municipio,
      e.orgao,
      e.id_unidade_gestora,
      e.id_licitacao_bd,
      CASE WHEN dlic = 1 THEN (SAFE_CAST (NULL AS STRING)) ELSE e.id_licitacao END AS id_licitacao,  
      e.modalidade_licitacao,
      e.id_empenho_bd,
      e.id_empenho,
      e.numero,
      e.descricao,
      e.modalidade,
      e.funcao,
      e.subfuncao,
      e.programa,
      e.acao,
      e.elemento_despesa,
      e.valor_inicial,
      e.valor_reforco,
      e.valor_anulacao,
      e.valor_ajuste,
      e.valor_final
    FROM empenhado_mg e
    LEFT JOIN dlic l ON l.id_empenho_bd = e.id_empenho_bd
),
  empenhado_pb AS (
    SELECT
      SAFE_CAST (dt_Ano AS INT64) AS ano,
      SAFE_CAST(SUBSTRING(TRIM(dt_empenho),-7,2) AS INT64) AS mes,
      SAFE_CAST (CONCAT(SUBSTRING(TRIM(dt_empenho),-4),'-',SUBSTRING(TRIM(dt_empenho),-7,2),'-',SUBSTRING(TRIM(dt_empenho),1,2))AS DATE) AS data,
      'PB' AS sigla_uf, 
      SAFE_CAST (m.id_municipio AS STRING) AS id_municipio,
      SAFE_CAST (e.cd_ugestora AS STRING) AS  orgao,
      SAFE_CAST (NULL AS STRING) AS id_unidade_gestora,
      SAFE_CAST (NULL AS STRING) AS id_licitacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_licitacao,
      SAFE_CAST (NULL AS STRING) AS modalidade_licitacao,
      SAFE_CAST (CONCAT(nu_Empenho, ' ', e.cd_ugestora, ' ', m.id_municipio, ' ', (RIGHT(dt_Ano,2))) AS STRING) AS id_empenho_bd,
      SAFE_CAST (NULL AS STRING) AS id_empenho,
      SAFE_CAST (nu_Empenho AS STRING) AS numero,
      SAFE_CAST (LOWER (de_Historico) AS STRING) AS descricao,
      SAFE_CAST (NULL AS STRING) AS modalidade,
      SAFE_CAST (SAFE_CAST (funcao AS INT64) AS STRING) AS funcao,
      SAFE_CAST (SAFE_CAST (subfuncao AS INT64) AS STRING) AS subfuncao,
      SAFE_CAST (de_Programa AS STRING) AS programa, --substituir por código
      SAFE_CAST (de_Acao AS STRING) AS acao, -- substituir por código
      CONCAT (
        CASE WHEN de_CatEconomica = 'Despesa Corrente'   THEN '3'
             WHEN de_CatEconomica = 'Despesa de Capital' THEN '4'
             WHEN de_CatEconomica = 'Reserva de Contingência' THEN '9'
             END,
        CASE WHEN de_NatDespesa = 'Pessoal e Encargos Sociais' THEN '1'
             WHEN de_NatDespesa = 'Juros e Encargos da Dívida' THEN '2'
             WHEN de_NatDespesa = 'Outras Despesas Correntes'  THEN '3'
             WHEN de_NatDespesa = 'Investimentos'              THEN '4'
             WHEN de_NatDespesa = 'Inversões Financeiras'      THEN '5'
             WHEN de_NatDespesa = 'Amortização da Dívida'      THEN '6'
             WHEN de_NatDespesa = 'Reserva de Contingência'    THEN '9'
             END,
        CASE WHEN de_Modalidade = 'Transferências à União'                                         THEN '20'
             WHEN de_Modalidade = 'Transferências a Instituições Privadas com Fins Lucrativos'     THEN '30'
             WHEN de_Modalidade = 'Execução Orçamentária Delegada a Estados e ao Distrito Federal' THEN '32'
             WHEN de_Modalidade = 'Aplicação Direta §§ 1º e 2º do Art. 24 LC 1412'                 THEN '35'
             WHEN de_Modalidade = 'Aplicação Direta Art. 25 LC 141'                                THEN '36'
             WHEN de_Modalidade = 'Transferências a Municípios'                                    THEN '40'
             WHEN de_Modalidade = 'Transferências a Municípios – Fundo a Fundo'                    THEN '41'
             WHEN de_Modalidade = 'Transferências a Instituições Privadas sem Fins Lucrativos'     THEN '50'
             WHEN de_Modalidade = 'Transferências a Instituições Privadas com Fins Lucrativos'     THEN '60'
             WHEN de_Modalidade = 'Transferências a Instituições Multigovernamentais'              THEN '70'
             WHEN de_Modalidade = 'Transf. a Consórc Púb. C.Rateio §§ 1º e 2º Art. 24  LC141'      THEN '71'
             WHEN de_Modalidade = 'Execução Orçamentária Delegada a Consórcios Públicos'           THEN '72'
             WHEN de_Modalidade = 'Transferências a Consórcios Públicos'                           THEN '73'
             WHEN de_Modalidade = 'Transf. a Consórc Púb. C.Rateio Art. 25 LC 141'                 THEN '74'
             WHEN de_Modalidade = 'Transferências ao Exterior'                                     THEN '80'
             WHEN de_Modalidade = 'Aplicações Diretas'                                             THEN '90'
             WHEN de_Modalidade = 'Ap. Direta Decor. de Op. entre Órg., Fundos e Ent. Integ. dos Orçamentos Fiscal e da Seguridade Social'                                                                                 THEN '91'
             WHEN de_Modalidade = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Participe'                                           THEN '93'
             WHEN de_Modalidade = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Não Participe'                                       THEN '94'
             ELSE NULL
             END,
        cd_elemento) AS elemento_despesa,
      SAFE_CAST (vl_Empenho AS FLOAT64) AS valor_inicial    
    FROM basedosdados-dev.world_wb_mides_staging.raw_empenho_pb e
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pb m ON e.cd_ugestora = SAFE_CAST(m.id_unidade_gestora AS STRING)
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_funcao f ON e.de_Funcao = f.nome_funcao
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_subfuncao sf ON e.de_Subfuncao = sf.nome_subfuncao
),
  anulacao_pb AS (
    SELECT
      SAFE_CAST (CONCAT(nu_Empenho, ' ', a.cd_ugestora, ' ', m.id_municipio, ' ', (RIGHT(dt_Ano,2))) AS STRING) AS id_empenho_bd,
      SUM(SAFE_CAST (vl_Estorno AS FLOAT64)) AS valor_anulacao
    FROM basedosdados-dev.world_wb_mides_staging.raw_estorno_pb a
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pb m ON a.cd_ugestora = SAFE_CAST(m.id_unidade_gestora AS STRING)
    GROUP BY 1
),
  frequencia_pb AS (
    SELECT id_empenho_bd, COUNT (id_empenho_bd) AS frequencia_id
    FROM empenhado_pb
    GROUP BY 1
),
  empenho_completo AS (
    SELECT
      e.ano,
      e.mes,
      e.data,
      e.sigla_uf,
      e.id_municipio,
      e.orgao,
      e.id_unidade_gestora,
      e.id_licitacao_bd,
      e.id_licitacao,
      e.modalidade_licitacao,
      e.id_empenho_bd,
      e.id_empenho,
      e.numero,
      e.descricao,
      e.modalidade,
      e.funcao,
      e.subfuncao,
      e.programa,
      e.acao,
      e.elemento_despesa,
      frequencia_id,
      ROUND(SUM(e.valor_inicial),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_reforco,
      ROUND(SUM(a.valor_anulacao/frequencia_id),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
    FROM empenhado_pb e
    FULL OUTER JOIN anulacao_pb a ON a.id_empenho_bd = e.id_empenho_bd
    LEFT JOIN frequencia_pb f ON f.id_empenho_bd = e.id_empenho_bd
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),
  empenho_pb AS (
    SELECT
      e.ano,
      e.mes,
      e.data,
      e.sigla_uf,
      e.id_municipio,
      e.orgao,
      e.id_unidade_gestora,
      e.id_licitacao_bd,
      e.id_licitacao,
      e.modalidade_licitacao,
      CASE WHEN (frequencia_id > 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE e.id_empenho_bd END AS id_empenho_bd,
      e.id_empenho,
      e.numero,
      e.descricao,
      e.modalidade,
      e.funcao,
      e.subfuncao,
      e.programa,
      e.acao,
      e.elemento_despesa,
      e.valor_inicial,
      e.valor_reforco,
      e.valor_anulacao,
      e.valor_ajuste,
      ROUND(e.valor_inicial - IFNULL(valor_anulacao, 0),2) AS valor_final
    FROM empenho_completo e
),
  empenho_pe AS (
    SELECT
      SAFE_CAST (e.ANOREFERENCIA AS INT64) AS ano,
      (SAFE_CAST(EXTRACT(MONTH FROM DATE (DATAEMPENHO)) AS INT64)) AS mes,
      SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(DATAEMPENHO)) AS DATE) AS data,
      'PE' AS sigla_uf, 
      SAFE_CAST (CODIGOIBGE AS STRING) AS id_municipio,
      SAFE_CAST (NULL AS STRING) orgao,
      SAFE_CAST (ID_UNIDADEGESTORA AS STRING) AS id_unidade_gestora,
      SAFE_CAST (NULL AS STRING) id_licitacao_bd,
      SAFE_CAST (NULL AS STRING) id_licitacao,
      SAFE_CAST (NULL AS STRING) modalidade_licitacao,
      SAFE_CAST (NULL AS STRING) AS id_empenho_bd,
      SAFE_CAST (TRIM(ID_EMPENHO) AS STRING) AS id_empenho,
      SAFE_CAST (e.NUMEROEMPENHO AS STRING) AS numero,
      SAFE_CAST (LOWER(HISTORICO) AS STRING) AS descricao,
      SAFE_CAST (LEFT(TIPO_EMPENHO, 1) AS STRING) AS modalidade,
      SAFE_CAST (SAFE_CAST (fun.funcao AS INT64) AS STRING) AS funcao,
      SAFE_CAST (SAFE_CAST (sub.subfuncao AS INT64) AS STRING) AS subfuncao,
      SAFE_CAST (PROGRAMA AS STRING) AS programa,
      SAFE_CAST (CODIGO_TIPO_ACAO AS STRING) AS acao,
      CONCAT (
        CASE WHEN CATEGORIA = 'Despesa Corrente'   THEN '3'
             WHEN CATEGORIA = 'Despesa de Capital' THEN '4'
             END,
        CASE WHEN NATUREZA = 'Pessoal e Encargos Sociais' THEN '1'
             WHEN NATUREZA = 'Juros e Encargos da Dívida' THEN '2'
             WHEN NATUREZA = 'Outras Despesas Correntes'  THEN '3'
             WHEN NATUREZA = 'Investimentos'              THEN '4'
             WHEN NATUREZA = 'Inversões Financeiras'      THEN '5'
             WHEN NATUREZA = 'Amortização da Dívida'      THEN '6'
             WHEN NATUREZA = 'Reserva de Contingência'    THEN '9'
             END,
        CASE WHEN MODALIDADE = 'Transferências à União'                                         THEN '20'
             WHEN MODALIDADE = 'Transferências a Instituições Privadas com Fins Lucrativos'     THEN '30'
             WHEN MODALIDADE = 'Execução Orçamentária Delegada a Estados e ao Distrito Federal' THEN '32'
             WHEN MODALIDADE = 'Aplicação Direta à conta de recursos de que tratam os §§ 1o e 2o do art. 24 da Lei Complementar no 141, de 2012'                                                                                THEN '35'
             WHEN MODALIDADE = 'Aplicação Direta à conta de recursos de que trata o art. 25 da Lei Complementar no 141, de 2012'                                                                                           THEN '36'
             WHEN MODALIDADE = 'Transferências a Municípios'                                    THEN '40'
             WHEN MODALIDADE = 'Transferências a Municípios – Fundo a Fundo'                    THEN '41'
             WHEN MODALIDADE = 'Transferências a Instituições Privadas sem Fins Lucrativos'     THEN '50'
             WHEN MODALIDADE = 'Transferências a Instituições Privadas com Fins Lucrativos'     THEN '60'
             WHEN MODALIDADE = 'Transferências a Instituições Multigovernamentais'              THEN '70'
             WHEN MODALIDADE = 'Transferências a Consórcios Públicos mediante contrato de rateio à conta de recursos de que tratam os §§ 1o e 2o do art. 24 da Lei Complementar no 141, de 2012'                            THEN '71'
             WHEN MODALIDADE = 'Execução Orçamentária Delegada a Consórcios Públicos'           THEN '72'
             WHEN MODALIDADE = 'Transferências a Consórcios Públicos'                           THEN '73'
             WHEN MODALIDADE = 'Transferências ao Exterior'                                     THEN '80'
             WHEN MODALIDADE = 'Aplicações Diretas'                                             THEN '90'
             WHEN MODALIDADE = 'Ap. Direta Decor. de Op. entre Órg., Fundos e Ent. Integ. dos Orçamentos Fiscal e da Seguridade Social'                                                                                         THEN '91'
             WHEN MODALIDADE = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Participe'                                        THEN '93'
             WHEN MODALIDADE = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Não Participe'                                    THEN '94'
             ELSE NULL
             END,
        CASE WHEN ELEMENTODESPESA = 'Pensões do RPPS e do militar'                                  THEN '03'
             WHEN ELEMENTODESPESA = 'Contratação por Tempo Determinado'                             THEN '04'
             WHEN ELEMENTODESPESA = 'Outros Benefícios Previdenciários do RPPS'                     THEN '05'
             WHEN ELEMENTODESPESA = 'Outros Benefícios Previdenciários do servidor ou do militar'   THEN '05'
             WHEN ELEMENTODESPESA = 'Beneficio Mensal ao Deficiente e ao Idoso'                     THEN '06'
             WHEN ELEMENTODESPESA = 'Contribuição a Entidades Fechadas de Previdência'              THEN '07'
             WHEN ELEMENTODESPESA = 'Outros Benefícios Assistenciais'                               THEN '08'
             WHEN ELEMENTODESPESA = 'Outros Benefícios Assistenciais do servidor e do militar'      THEN '08'
             WHEN ELEMENTODESPESA = 'Salário Família'                                               THEN '09'
             WHEN ELEMENTODESPESA = 'Seguro Desemprego e Abono Salarial'                            THEN '10'
             WHEN ELEMENTODESPESA = 'Vencimentos e Vantagens Fixas - Pessoal Civil'                 THEN '11'
             WHEN ELEMENTODESPESA = 'Vencimentos e Vantagens Fixas - Pessoal Militar'               THEN '12'
             WHEN ELEMENTODESPESA = 'Obrigações Patronais'                                          THEN '13'
             WHEN ELEMENTODESPESA = 'Aporte para Cobertura do Déficit Atuarial do RPPS'             THEN '13'
             WHEN ELEMENTODESPESA = 'Diárias - Civil'                                               THEN '14'
             WHEN ELEMENTODESPESA = 'Outras Despesas Variáveis - Pessoal Civil'                     THEN '16'
             WHEN ELEMENTODESPESA = 'Auxílio Financeiro a Estudantes'                               THEN '18'
             WHEN ELEMENTODESPESA = 'Auxílio Fardamento'                                            THEN '19'
             WHEN ELEMENTODESPESA = 'Auxílio Financeiro a Pesquisadores'                            THEN '20'
             WHEN ELEMENTODESPESA = 'Outros Encargos sobre a Dívida por Contrato'                   THEN '22'
             WHEN ELEMENTODESPESA = 'Juros, Deságios e Descontos da Dívida Mobiliária'              THEN '23'
             WHEN ELEMENTODESPESA = 'Outros Encargos sobre a Dívida Mobiliária'                     THEN '24'
             WHEN ELEMENTODESPESA = 'Encargos sobre Operações de Crédito por Antecipação da Receita'THEN '25'
             WHEN ELEMENTODESPESA = 'Encargos pela Honra de Avais, Garantias, Seguros e Similares'                                                                                          THEN '27'
             WHEN ELEMENTODESPESA = 'Remuneração de Cotas de Fundos Autárquicos'                    THEN '28'
             WHEN ELEMENTODESPESA = 'Material de Consumo'                                           THEN '30'
             WHEN ELEMENTODESPESA = 'Premiações Culturais, Artísticas, Científicas, Desportivas e Outras'                                                                                             THEN '31'
             WHEN ELEMENTODESPESA = 'Material, Bem ou Serviço para Distribuição Gratuita'           THEN '32'
             WHEN ELEMENTODESPESA = 'Passagens e Despesas de Locomoção'                             THEN '33'
             WHEN ELEMENTODESPESA = 'Outras Despesas de Pessoal decorrentes de Contratos de Terceirização'                                                                                      THEN '34'
             WHEN ELEMENTODESPESA = 'Serviços de Consultoria'                                       THEN '35'
             WHEN ELEMENTODESPESA = 'Locação de Mão-de-Obra'                                        THEN '37'
             WHEN ELEMENTODESPESA = 'Outros Serviços de Terceiros ? Pessoa Jurídica'                THEN '39'
             WHEN ELEMENTODESPESA = 'Serviços de Tecnologia da Informação e Comunicação - Pessoa Jurídica'                                                                                           THEN '40'
             WHEN ELEMENTODESPESA = 'Serviços de Tecnologia da Informação e Comunicação ? Pessoa Jurídica'                                                                                           THEN '40'
             WHEN ELEMENTODESPESA = 'Contribuições'                                                 THEN '41'
             WHEN ELEMENTODESPESA = 'Auxílios'                                                      THEN '42'
             WHEN ELEMENTODESPESA = 'Obrigações Tributárias e Contributivas'                        THEN '47'
             WHEN ELEMENTODESPESA = 'Auxílio-Transporte'                                            THEN '49'
             WHEN ELEMENTODESPESA = 'Obras e Instalações'                                           THEN '51'
             WHEN ELEMENTODESPESA = 'Equipamentos e Material Permanente'                            THEN '52'
             WHEN ELEMENTODESPESA = 'Aposentadorias do RGPS ? Área Urbana'                          THEN '54'
             WHEN ELEMENTODESPESA = 'Pensões, exclusiva do RGPS'                                    THEN '56'
             WHEN ELEMENTODESPESA = 'Outros Benefícios do RGPS ? Área Urbana'                       THEN '58'
             WHEN ELEMENTODESPESA = 'Pensões Especiais'                                             THEN '59'
             WHEN ELEMENTODESPESA = 'Aquisição de Imóveis'                                          THEN '61'
             WHEN ELEMENTODESPESA = 'Constituição ou Aumento de Capital de Empresas'                THEN '65'
             WHEN ELEMENTODESPESA = 'Concessão de Empréstimos e Financiamentos'                     THEN '66'
             WHEN ELEMENTODESPESA = 'Depósitos Compulsórios'                                        THEN '67'
             WHEN ELEMENTODESPESA = 'Rateio pela Participação em Consórcio Público'                 THEN '70'
             WHEN ELEMENTODESPESA = 'Principal da Dívida Contratual Resgatado'                      THEN '71'
             WHEN ELEMENTODESPESA = 'Principal da Dívida Mobiliária Resgatado'                      THEN '72'
             WHEN ELEMENTODESPESA = 'Correção Monetária ou Cambial da Dívida Contratual Resgatada'  THEN '73'
             WHEN ELEMENTODESPESA = 'Principal Corrigido da Dívida Contratual Refinanciado'         THEN '77'
             WHEN ELEMENTODESPESA = 'Distribuição Constitucional ou Legal de Receitas'              THEN '81'
             WHEN ELEMENTODESPESA = 'Sentenças Judiciais'                                           THEN '91'
             WHEN ELEMENTODESPESA = 'Despesas de Exercícios Anteriores'                             THEN '92'
             WHEN ELEMENTODESPESA = 'Indenizações e Restituições'                                   THEN '93'
             WHEN ELEMENTODESPESA = 'Indenização pela Execução de Trabalhos de Campo'               THEN '95'
             WHEN ELEMENTODESPESA = 'Ressarcimento de Despesas de Pessoal Requisitado'              THEN '96'
             ELSE NULL
             END
            ) AS elemento_despesa,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_reforco,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (VALOREMPENHADO AS FLOAT64),2) AS valor_final
    FROM basedosdados-dev.world_wb_mides_staging.raw_empenho_pe e
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_pe m ON e.NOMEUNIDADEGESTORA = m.NOMEUNIDADEGESTORA
    LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_funcao` fun ON UPPER(TRIM(REPLACE(REPLACE(e.FUNCAO, 'Encargos Especias', 'Encargos Especiais'), 'Assistêncial Social', 'Assistência Social'))) = UPPER(nome_funcao) 
    LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_subfuncao` sub ON UPPER(TRIM(e.SUBFUNCAO)) = UPPER(nome_subfuncao)
),
  empenho_pr AS (
    SELECT
      SAFE_CAST (nrAnoEmpenho AS INT64) AS ano,
      (SAFE_CAST(EXTRACT(MONTH FROM DATE (dtEmpenho)) AS INT64)) AS mes,
      SAFE_CAST (EXTRACT(DATE FROM TIMESTAMP(dtEmpenho)) AS DATE) AS data,
      'PR'AS sigla_uf, 
      SAFE_CAST (m.id_municipio AS STRING) AS id_municipio,
      SAFE_CAST (TRIM(cdOrgao, '0') AS STRING) AS  orgao,
      SAFE_CAST (cdUnidade AS STRING) AS id_unidade_gestora,
      SAFE_CAST (NULL AS STRING) AS id_licitacao_bd,
      SAFE_CAST (NULL AS STRING) AS id_licitacao,
      SAFE_CAST (NULL AS STRING) AS modalidade_licitacao,
      SAFE_CAST (CONCAT(idEmpenho, ' ', m.id_municipio) AS STRING) AS id_empenho_bd,
      SAFE_CAST (idEmpenho AS STRING) AS id_empenho,
      SAFE_CAST (nrEmpenho AS STRING) AS numero,
      SAFE_CAST (LOWER (dsHistorico) AS STRING) AS descricao,
      SAFE_CAST (LEFT(dsTipoEmpenho, 1) AS STRING) AS modalidade,
      SAFE_CAST (SAFE_CAST (cdFuncao AS INT64) AS STRING) AS funcao,
      SAFE_CAST (SAFE_CAST (cdSubFuncao AS INT64) AS STRING) AS subfuncao,
      SAFE_CAST (SAFE_CAST (cdPrograma AS INT64) AS STRING) AS programa,
      SAFE_CAST (SAFE_CAST (cdProjetoAtividade AS INT64) AS STRING) AS acao,
      SAFE_CAST (CONCAT (cdCategoriaEconomica, cdGrupoNatureza, cdModalidade, cdElemento) AS STRING) AS elemento_despesa,
      ROUND(SAFE_CAST (vlEmpenho AS FLOAT64),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_reforco,
      ROUND(SAFE_CAST (vlEstornoEmpenho AS FLOAT64),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SAFE_CAST (vlEmpenho AS FLOAT64) - IFNULL(SAFE_CAST (vlEstornoEmpenho AS FLOAT64),0),2) AS valor_final
    FROM basedosdados-dev.world_wb_mides_staging.raw_empenho_pr e
    LEFT JOIN basedosdados.br_bd_diretorios_brasil.municipio m ON e.cdIBGE = m.id_municipio_6
),
  empenhado_rs AS (
   SELECT
       MIN(ano_recebimento) AS ano_recebimento,
       SAFE_CAST(ano_empenho AS INT64) AS ano,
       SAFE_CAST(EXTRACT(MONTH FROM DATE(dt_operacao)) AS INT64) AS mes,
       SAFE_CAST(CONCAT(SUBSTRING(dt_operacao,1,4), '-', SUBSTRING(dt_operacao,6,2),  '-', SUBSTRING(dt_operacao,9,2)) AS DATE) AS data,
       'RS' AS sigla_uf,
       SAFE_CAST(a.id_municipio AS STRING) AS id_municipio,
       SAFE_CAST(c.cd_orgao AS STRING) AS orgao,
       SAFE_CAST(cd_orgao_orcamentario AS STRING) AS id_unidade_gestora,
       SAFE_CAST(NULL AS STRING) AS id_licitacao_bd,
       SAFE_CAST(NULL AS STRING) AS id_licitacao,
       SAFE_CAST(NULL AS STRING) AS modalidade_licitacao,
       SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
       SAFE_CAST(NULL AS STRING) AS id_empenho,
       SAFE_CAST(nr_empenho AS STRING) AS numero,
       SAFE_CAST(LOWER (historico) AS STRING) AS descricao,
       SAFE_CAST(NULL AS STRING) AS modalidade,
       SAFE_CAST(SAFE_CAST (cd_funcao AS INT64) AS STRING) AS funcao,
       SAFE_CAST(SAFE_CAST (cd_subfuncao AS INT64) AS STRING) AS subfuncao,
       SAFE_CAST(SAFE_CAST (cd_programa AS INT64) AS STRING) AS programa,
       SAFE_CAST(SAFE_CAST (cd_projeto AS INT64) AS STRING) AS acao,
       SAFE_CAST(REPLACE(cd_elemento, '.','') AS STRING) AS elemento_despesa,
       SAFE_CAST(vl_empenho AS FLOAT64) AS valor_inicial     
     FROM `basedosdados-dev.world_wb_mides_staging.raw_despesa_rs` AS c
     LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
     LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
     WHERE tipo_operacao = 'E' AND (SAFE_CAST(vl_empenho AS FLOAT64) >= 0)
     GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
),
  frequencia_rs AS (
    SELECT
      id_empenho_bd,
      COUNT(id_empenho_bd) AS frequencia_id
    FROM empenhado_rs
    GROUP BY 1
),
  anulacao_rs AS (
    SELECT 
      SAFE_CAST(CONCAT(nr_empenho, ' ', c.cd_orgao, ' ', m.id_municipio, ' ', (RIGHT(ano_empenho,2))) AS STRING) AS id_empenho_bd,
      -1*SUM(SAFE_CAST(vl_empenho AS FLOAT64)) AS valor_anulacao
    FROM `basedosdados-dev.world_wb_mides_staging.raw_despesa_rs` AS c
    LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_orgao_rs` AS a ON c.cd_orgao = a.cd_orgao
    LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m ON m.id_municipio = a.id_municipio
    WHERE tipo_operacao='E' AND (SAFE_CAST(vl_empenho AS FLOAT64) < 0)
    GROUP BY 1   
),
  empenho_anulacao AS (
    SELECT 
      e.*,
      f.frequencia_id,
      a.valor_anulacao/f.frequencia_id AS valor_anulacao
    FROM empenhado_rs e
    LEFT JOIN anulacao_rs a ON e.id_empenho_bd = a.id_empenho_bd
    LEFT JOIN frequencia_rs f ON e.id_empenho_bd = f.id_empenho_bd
),
  dorgao AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT orgao)) > 1 THEN 1 ELSE 0 END AS dorgao
    FROM empenho_anulacao
    GROUP BY 1
),
  dugest AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT id_unidade_gestora)) > 1 THEN 1 ELSE 0 END AS dugest
    FROM empenho_anulacao
    GROUP BY 1
),
  ddesc AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT descricao)) > 1 THEN 1 ELSE 0 END AS ddesc
    FROM empenho_anulacao
    GROUP BY 1
),
  dfun AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT funcao)) > 1 THEN 1 ELSE 0 END AS dfun
    FROM empenho_anulacao
    GROUP BY 1
),
  dsubf AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT subfuncao)) > 1 THEN 1 ELSE 0 END AS dsubf
    FROM empenho_anulacao
    GROUP BY 1
),
  dprog AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT programa)) > 1 THEN 1 ELSE 0 END AS dprog
    FROM empenho_anulacao
    GROUP BY 1
),
  dacao AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT acao)) > 1 THEN 1 ELSE 0 END AS dacao
    FROM empenho_anulacao
    GROUP BY 1
),
  delem AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT elemento_despesa)) > 1 THEN 1 ELSE 0 END AS delem
    FROM empenho_anulacao
    GROUP BY 1
),
  dummies AS (
    SELECT 
      o.id_empenho_bd,
      dorgao,
      dugest,
      ddesc,
      dfun,
      dsubf,
      dprog,
      dacao,
      delem  
    FROM dorgao o
    LEFT JOIN dugest g ON o.id_empenho_bd = g.id_empenho_bd
    LEFT JOIN ddesc d ON o.id_empenho_bd = d.id_empenho_bd
    LEFT JOIN dfun f ON o.id_empenho_bd = f.id_empenho_bd
    LEFT JOIN dsubf s ON o.id_empenho_bd = s.id_empenho_bd
    LEFT JOIN dprog p ON o.id_empenho_bd = p.id_empenho_bd
    LEFT JOIN dacao a ON o.id_empenho_bd = a.id_empenho_bd
    LEFT JOIN delem e ON o.id_empenho_bd = e.id_empenho_bd
),
  empenho_rs AS (
    SELECT  
      MIN(e.ano) AS ano,
      MIN(e.mes) AS mes,
      MIN(e.data) AS data,
      e.sigla_uf,
      e.id_municipio,
      e.orgao,
      e.id_unidade_gestora,
      e.id_licitacao_bd,
      e.id_licitacao,
      e.modalidade_licitacao,
      (CASE WHEN (dorgao = 1 OR dugest = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE e.id_empenho_bd END) AS id_empenho_bd,  
      e.id_empenho,
      e.numero,
      (CASE WHEN (ddesc = 1 AND (dorgao = 0 OR dugest = 0 OR dfun = 0 OR dsubf = 0 OR dprog = 0 OR dacao = 0 OR delem = 0)) THEN (SAFE_CAST (NULL AS STRING))
            WHEN (ddesc = 1 AND (dorgao = 1 OR dugest = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1)) THEN (SAFE_CAST (e.descricao AS STRING))  
      ELSE e.descricao END) AS descricao,
      e.modalidade,
      e.funcao,
      e.subfuncao,
      e.programa,
      e.acao,
      e.elemento_despesa,
      ROUND(SUM(e.valor_inicial),2) AS valor_inicial,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_reforco,
      ROUND(SUM(e.valor_anulacao),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(SUM(e.valor_inicial) - IFNULL(SUM(e.valor_anulacao),0),2) AS valor_final
    FROM empenho_anulacao e
    LEFT JOIN dummies d ON e.id_empenho_bd = d.id_empenho_bd
    GROUP BY 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),
  empenhado_sp AS (
   SELECT
     SAFE_CAST (ano_exercicio AS INT64) AS ano,
     SAFE_CAST (mes_referencia AS INT64) AS mes,
     SAFE_CAST (CONCAT(SUBSTRING(dt_emissao_despesa,-4),'-',SUBSTRING(dt_emissao_despesa,-7,2),'-',SUBSTRING(dt_emissao_despesa,1,2)) AS DATE) AS data,
     'SP' AS sigla_uf,
     SAFE_CAST (id_municipio AS STRING) AS id_municipio,
     SAFE_CAST (codigo_orgao AS STRING) AS orgao,
     SAFE_CAST (NULL AS STRING) AS id_unidade_gestora,
     SAFE_CAST (NULL AS STRING) AS id_licitacao_bd,
     SAFE_CAST (NULL AS STRING) AS id_licitacao,
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
     SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_empenho_bd,
     SAFE_CAST (NULL AS STRING) AS id_empenho,
     SAFE_CAST (nr_empenho AS STRING) AS numero,
     SAFE_CAST (LOWER (historico_despesa) AS STRING) AS descricao,
     SAFE_CAST (NULL AS STRING) AS modalidade,
     SAFE_CAST (SAFE_CAST (funcao AS INT64) AS STRING) AS funcao,
     SAFE_CAST (SAFE_CAST (subfuncao AS INT64) AS STRING) AS subfuncao,
     SAFE_CAST (SAFE_CAST (cd_programa AS INT64) AS STRING) AS programa,
     SAFE_CAST (SAFE_CAST (cd_acao AS INT64) AS STRING) AS acao,
     SAFE_CAST ((LEFT(ds_elemento,8)) AS STRING) AS elemento_despesa,
     SAFE_CAST (REPLACE(vl_despesa, ',', '.') AS FLOAT64) AS valor_inicial
   FROM basedosdados-dev.world_wb_mides_staging.raw_despesa_sp e
   LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_sp m ON m.ds_orgao = e.ds_orgao
   LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_funcao` ON ds_funcao_governo = UPPER(nome_funcao)
   LEFT JOIN `basedosdados-dev.world_wb_mides_staging.aux_subfuncao` ON ds_subfuncao_governo = UPPER(nome_subfuncao)
   WHERE tp_despesa = 'Empenhado'
),
  frequencia_sp AS (
     SELECT id_empenho_bd, COUNT (id_empenho_bd) AS frequencia_id
     FROM empenhado_sp
     GROUP BY 1
     ORDER BY 2 DESC
),  
  anulacao AS (
    SELECT
      SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_empenho_bd,           
      SUM(SAFE_CAST (REPLACE(vl_despesa, ',', '.') AS FLOAT64)) AS valor_anulacao
    FROM basedosdados-dev.world_wb_mides_staging.raw_despesa_sp a
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_sp m ON m.ds_orgao = a.ds_orgao
    WHERE tp_despesa = 'Anulação'
    GROUP BY 1
),
  reforco AS (
    SELECT
      SAFE_CAST (CONCAT(LEFT(nr_empenho, LENGTH(nr_empenho) - 5), ' ', codigo_orgao, ' ', id_municipio, ' ', (RIGHT(ano_exercicio,2))) AS STRING) AS id_empenho_bd,
      SUM(SAFE_CAST (REPLACE(vl_despesa, ',', '.') AS FLOAT64)) AS valor_reforco
    FROM basedosdados-dev.world_wb_mides_staging.raw_despesa_sp r
    LEFT JOIN basedosdados-dev.world_wb_mides_staging.aux_municipio_sp m ON m.ds_orgao = r.ds_orgao
    WHERE tp_despesa = 'Reforço'
    GROUP BY 1
),
  empenho_completo_sp AS (
    SELECT 
      e.*,
      r.valor_reforco/frequencia_id AS valor_reforco,
      a.valor_anulacao/frequencia_id AS valor_anulacao,
    FROM empenhado_sp e
    LEFT JOIN frequencia_sp f ON e.id_empenho_bd = f.id_empenho_bd
    LEFT JOIN anulacao a ON e.id_empenho_bd = a.id_empenho_bd
    LEFT JOIN reforco r ON e.id_empenho_bd = r.id_empenho_bd
),
  dorgao_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT orgao)) > 1 THEN 1 ELSE 0 END AS dorgao
    FROM empenho_completo_sp
    GROUP BY 1
),
  ddesc_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT IFNULL(descricao,''))) > 1 THEN 1 ELSE 0 END AS ddesc
    FROM empenho_completo_sp
    GROUP BY 1
),
  dmod_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT modalidade_licitacao)) > 1 THEN 1 ELSE 0 END AS dmod
    FROM empenho_completo_sp
    GROUP BY 1
),
  dfun_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT funcao)) > 1 THEN 1 ELSE 0 END AS dfun
    FROM empenho_completo_sp
    GROUP BY 1
),
  dsubf_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT subfuncao)) > 1 THEN 1 ELSE 0 END AS dsubf
    FROM empenho_completo_sp
    GROUP BY 1
),
  dprog_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT programa)) > 1 THEN 1 ELSE 0 END AS dprog
    FROM empenho_completo_sp
    GROUP BY 1
),
  dacao_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT acao)) > 1 THEN 1 ELSE 0 END AS dacao
    FROM empenho_completo_sp
    GROUP BY 1
),
  delem_sp AS (
    SELECT 
      id_empenho_bd,
      CASE WHEN (COUNT (DISTINCT elemento_despesa)) > 1 THEN 1 ELSE 0 END AS delem
    FROM empenho_completo_sp
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
      delem  
    FROM dorgao_sp o
    FULL OUTER JOIN dmod_sp m ON o.id_empenho_bd = m.id_empenho_bd
    FULL OUTER JOIN ddesc_sp d ON o.id_empenho_bd = d.id_empenho_bd
    FULL OUTER JOIN dfun_sp f ON o.id_empenho_bd = f.id_empenho_bd
    FULL OUTER JOIN dsubf_sp s ON o.id_empenho_bd = s.id_empenho_bd
    FULL OUTER JOIN dprog_sp p ON o.id_empenho_bd = p.id_empenho_bd
    FULL OUTER JOIN dacao_sp a ON o.id_empenho_bd = a.id_empenho_bd
    FULL OUTER JOIN delem_sp e ON o.id_empenho_bd = e.id_empenho_bd
),
  empenho_sp AS (
    SELECT
      MIN(ano) AS ano,
      MIN(mes) AS mes,
      MIN(data) AS data,
      sigla_uf,
      id_municipio,
      orgao,
      id_unidade_gestora,
      id_licitacao_bd,
      id_licitacao,
      modalidade_licitacao,
      (CASE WHEN (dorgao = 1 OR dmod = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1) THEN (SAFE_CAST (NULL AS STRING)) ELSE e.id_empenho_bd END) AS id_empenho_bd,
      id_empenho,  
      numero,
      CASE WHEN (ddesc = 1 AND (dorgao = 0 OR dmod = 0 OR dfun = 0 OR dsubf = 0 OR dprog = 0 OR dacao = 0 OR delem = 0)) THEN (SAFE_CAST (NULL AS STRING))
          WHEN (ddesc = 1 AND (dorgao = 1 OR dmod = 1 OR dfun = 1 OR dsubf = 1 OR dprog = 1 OR dacao = 1 OR delem = 1)) THEN (SAFE_CAST (e.descricao AS STRING))  
      ELSE e.descricao END AS descricao,
      modalidade,
      funcao,
      subfuncao,
      programa,
      acao,
      elemento_despesa, 
      ROUND(SUM(valor_inicial),2) AS valor_inicial,
      ROUND(SUM(valor_reforco),2) AS valor_reforco,
      ROUND(SUM(valor_anulacao),2) AS valor_anulacao,
      ROUND(SAFE_CAST (0 AS FLOAT64),2) AS valor_ajuste,
      ROUND(IFNULL(SUM(valor_inicial),0) + IFNULL(SUM(valor_reforco),0) - IFNULL(SUM(valor_anulacao),0),2) AS valor_final
    FROM empenho_completo_sp e
    LEFT JOIN dummies_sp d ON d.id_empenho_bd=e.id_empenho_bd
    GROUP BY 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)

SELECT 
  *
FROM empenho_mg
UNION ALL (SELECT * FROM empenho_sp)
UNION ALL (SELECT * FROM empenho_pe)
UNION ALL (SELECT * FROM empenho_pr)
UNION ALL (SELECT * FROM empenho_rs)
UNION ALL (SELECT * FROM empenho_pb)
UNION ALL (SELECT * FROM empenho_ce)
)