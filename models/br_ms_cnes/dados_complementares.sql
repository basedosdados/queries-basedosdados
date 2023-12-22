{{ 
  config(
    schema='br_ms_cnes',
    materialized='incremental',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2005,
        "end": 2024,
        "interval": 1}
     },
     pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
     post_hook = [ 
      'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
      'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
       ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)'      
     ]   
    )
 }}
WITH raw_cnes_dados_complementares AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.dados_complementares`
  WHERE CNES IS NOT NULL
),
raw_cnes_dados_complementares_without_duplicates as(
  -- 2. distinct nas linhas 
  SELECT DISTINCT *
  FROM raw_cnes_dados_complementares
),
cnes_add_muni AS (
  -- 3. Adicionar id_municipio e sigla_uf
  SELECT *
  FROM raw_cnes_dados_complementares_without_duplicates  
  LEFT JOIN (SELECT id_municipio, id_municipio_6,
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) as mun
  ON raw_cnes_dados_complementares_without_duplicates.CODUFMUN = mun.id_municipio_6
)

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(CNES AS STRING) id_estabelecimento_cnes,
SAFE_CAST(CNS_ADM AS STRING) cns_medico_responsavel_administrador_responsavel_tecnico,
SAFE_CAST(CNS_OPED AS STRING) cns_medico_responsavel_oncologista_pediatrico,
SAFE_CAST(CNS_CONC AS STRING) cns_medico_responsavel_cirurgia_oncologica,
SAFE_CAST(CNS_OCLIN AS STRING) cns_medico_responsavel_oncologista_clinico,
SAFE_CAST(CNS_MRAD AS STRING) cns_medico_responsavel_radioterapeuta,
SAFE_CAST(CNS_FNUC AS STRING) cns_medico_responsavel_fisico_nuclear,
SAFE_CAST(CNS_NEFR AS STRING) cns_medico_responsavel_nefrologista,
SAFE_CAST(CNS_HMTR AS STRING) cns_medico_responsavel_hemoterapeuta,
SAFE_CAST(CNS_HMTL AS STRING) cns_medico_responsavel_hematologista,
SAFE_CAST(CNS_CRES AS STRING) cns_medico_capacitado_responsavel,
SAFE_CAST(CNS_RTEC AS STRING) cns_responsavel_tecnico_sorologia,
SAFE_CAST(S_HBSAGP AS INT64) quantidade_salas_hbsag_positivo,
SAFE_CAST(S_HBSAGN AS INT64) quantidade_salas_hbsag_negativo,
SAFE_CAST(S_DPI AS INT64) quantidade_salas_dpi,
SAFE_CAST(S_DPAC AS INT64) quantidade_salas_dpac,
SAFE_CAST(S_REAGP AS INT64) quantidade_salas_reuso_hbsag_positivo,
SAFE_CAST(S_REAGN AS INT64) quantidade_salas_reuso_hbsag_negativo,
SAFE_CAST(S_REHCV AS INT64) quantidade_salas_reuso_hcv_positivo,
SAFE_CAST(MAQ_PROP AS INT64) quantidade_maquinas_proporcao,
SAFE_CAST(MAQ_OUTR AS INT64) quantidade_outras_maquinas,
SAFE_CAST(SIMUL_RD AS INT64) quantidade_salas_simulacao_radioterapia,
SAFE_CAST(PLANJ_RD AS INT64) quantidade_salas_planejamento_radioterapia,
SAFE_CAST(ARMAZ_FT AS INT64) quantidade_salas_armazenamento_fontes_radioterapia,
SAFE_CAST(CONF_MAS AS INT64) quantidade_salas_confeccao_masc_radioterapia,
SAFE_CAST(SALA_MOL AS INT64) quantidade_salas_molde_radioterapia,
SAFE_CAST(BLOCOPER AS INT64) quantidade_salas_bloco_personalizado_radioterapia,
SAFE_CAST(S_ARMAZE AS INT64) quantidade_salas_armazenagem,
SAFE_CAST(S_PREPAR AS INT64) quantidade_salas_preparo,
SAFE_CAST(S_QCDURA AS INT64) quantidade_salas_equipamentos_quimio_curta_duracao,
SAFE_CAST(S_QLDURA AS INT64) quantidade_salas_equipamentos_quimio_longa_duracao,
SAFE_CAST(S_CPFLUX AS INT64) quantidade_salas_equipamentos_capela_fluxo_laminar,
SAFE_CAST(S_SIMULA AS INT64) quantidade_simuladores,
SAFE_CAST(S_ACELL6 AS INT64) quantidade_acelerador_linear_ate_6_mev,
SAFE_CAST(S_ALSEME AS INT64) quantidade_acelerador_linear_maior_6_mev_sem_eletrons,
SAFE_CAST(S_ALCOME AS INT64) quantidade_acelerador_linear_maior_6_mev_com_eletrons,
SAFE_CAST(ORTV1050 AS INT64) quantidade_equipamentos_ortovoltagem_10_50_kv,
SAFE_CAST(ORV50150 AS INT64) quantidade_equipamentos_ortovoltagem_50_150_kv,
SAFE_CAST(OV150500 AS INT64) quantidade_equipamentos_ortovoltagem_150_500_kv,
SAFE_CAST(UN_COBAL AS INT64) quantidade_unidade_cobalto,
SAFE_CAST(EQBRBAIX AS INT64) quantidade_equipamentos_braquiterapia_baixa,
SAFE_CAST(EQBRMEDI AS INT64) quantidade_equipamentos_braquiterapia_media,
SAFE_CAST(EQBRALTA AS INT64) quantidade_equipamentos_braquiterapia_alta,
SAFE_CAST(EQ_MAREA AS INT64) quantidade_monitor_area,
SAFE_CAST(EQ_MINDI AS INT64) quantidade_monitor_individual,
SAFE_CAST(EQSISPLN AS INT64) quantidade_sistema_computacao_planejamento,
SAFE_CAST(EQDOSCLI AS INT64) quantidade_dosimetro_clinico,
SAFE_CAST(EQFONSEL AS INT64) quantidade_fontes_seladas,
SAFE_CAST(S_RECEPC AS INT64) quantidade_salas_recepcao,
SAFE_CAST(S_TRIHMT AS INT64) quantidade_salas_triagem_hematologica,
SAFE_CAST(S_TRICLI AS INT64) quantidade_salas_triagem_clinica,
SAFE_CAST(S_COLETA AS INT64) quantidade_salas_coleta,
SAFE_CAST(S_AFERES AS INT64) quantidade_salas_aferese,
SAFE_CAST(S_PREEST AS INT64) quantidade_salas_pre_estoque,
SAFE_CAST(S_PROCES AS INT64) quantidade_salas_processamento,
SAFE_CAST(S_ESTOQU AS INT64) quantidade_salas_estoque,
SAFE_CAST(S_DISTRI AS INT64) quantidade_salas_distribuicao,
SAFE_CAST(S_SOROLO AS INT64) quantidade_salas_sorologia,
SAFE_CAST(S_IMUNOH AS INT64) quantidade_salas_imunohematologia,
SAFE_CAST(S_PRETRA AS INT64) quantidade_salas_pre_transfusionais,
SAFE_CAST(S_HEMOST AS INT64) quantidade_salas_hemostasia,
SAFE_CAST(S_CONTRQ AS INT64) quantidade_salas_controle_qualidade,
SAFE_CAST(S_BIOMOL AS INT64) quantidade_salas_biologia_molecular,
SAFE_CAST(S_IMUNFE AS INT64) quantidade_salas_imunofenotipagem,
SAFE_CAST(S_TRANSF AS INT64) quantidade_salas_transfusao,
SAFE_CAST(S_SGDOAD AS INT64) quantidade_salas_seguimento_doador,
SAFE_CAST(QT_CADRE AS INT64) quantidade_cadeiras_reclinaveis,
SAFE_CAST(QT_CENRE AS INT64) quantidade_centrifugas_refrigeradas,
SAFE_CAST(QT_REFSA AS INT64) quantidade_refrigeradores_guarda_sangue,
SAFE_CAST(QT_CONRA AS INT64) quantidade_congeladores_rapidos,
SAFE_CAST(QT_EXTPL AS INT64) quantidade_extratores_automaticos_plasma,
SAFE_CAST(QT_FRE18 AS INT64) quantidade_freezers_18_graus_celsius,
SAFE_CAST(QT_FRE30 AS INT64) quantidade_freezers_30_graus_celsius,
SAFE_CAST(QT_AGIPL AS INT64) quantidade_agitadores_plaquetas,
SAFE_CAST(QT_SELAD AS INT64) quantidade_seladoras,
SAFE_CAST(QT_IRRHE AS INT64) quantidade_irradiadores_hemocomponentes,
SAFE_CAST(QT_AGLTN AS INT64) quantidade_aglutinoscopio,
SAFE_CAST(QT_MAQAF AS INT64) quantidade_maquinas_aferese,
SAFE_CAST(QT_REFRE AS INT64) quantidade_refrigeradores_reagentes,
SAFE_CAST(QT_REFAS AS INT64) quantidade_refrigeradores_amostras_sangue,
SAFE_CAST(QT_CAPFL AS INT64) quantidade_capelas_fluxo_laminar,
SAFE_CAST(HEMOTERA AS INT64) indicador_existencia_requisito_hemoterapia,
SAFE_CAST(F_AREIA AS INT64) indicador_tratamento_agua_filtro_areia,
SAFE_CAST(F_CARVAO AS INT64) indicador_tratamento_agua_filtro_carvao,
SAFE_CAST(ABRANDAD AS INT64) indicador_tratamento_agua_abrandador,
SAFE_CAST(DEIONIZA AS INT64) indicador_tratamento_agua_deionizador,
SAFE_CAST(OSMOSE_R AS INT64) indicador_tratamento_agua_maquina_osmose,
SAFE_CAST(OUT_TRAT AS INT64) indicador_tratamento_agua_outros_equipamentos,
SAFE_CAST(DIALISE AS INT64) indicador_existencia_requisito_dialise,
SAFE_CAST(QUIMRADI AS INT64) indicador_existencia_requisito_quimio_radio
FROM cnes_add_muni AS t
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}

