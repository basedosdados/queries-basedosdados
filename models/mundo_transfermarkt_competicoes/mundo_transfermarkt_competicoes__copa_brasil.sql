{{ 
  config(
    alias='copa_brasil',
    schema='mundo_transfermarkt_competicoes',
    materialized='table',
     partition_by={
      "field": "ano_campeonato",
      "data_type": "int64",
      "range": {
        "start": 2020,
        "end": 2022,
        "interval": 1}
    },
    labels =  {'tema': 'esporte'},
    post_hook = ['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), MONTH) > 6)',
          'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                ON  {{this}}
                GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), MONTH) <= 6)' ]
    )
 }}

SELECT 
  SAFE_CAST(REPLACE (ano_campeonato,".0","") AS INT64) ano_campeonato,
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(CONCAT(horario, ":00") AS TIME) horario,
  INITCAP(fase) fase,
  INITCAP(tipo_fase) tipo_fase,
  SAFE_CAST(estadio AS STRING) estadio,
  SAFE_CAST(arbitro AS STRING) arbitro,
  SAFE_CAST(REPLACE (publico,".0","") AS INT64) publico,
  SAFE_CAST(REPLACE (publico_max,".0","") AS INT64) publico_max,
  SAFE_CAST(time_man AS STRING) time_mandante,
  SAFE_CAST(time_vis AS STRING) time_visitante,
  SAFE_CAST(tecnico_man AS STRING) tecnico_mandante,
  SAFE_CAST(tecnico_vis AS STRING) tecnico_visitante,
  SAFE_CAST(REPLACE (valor_equipe_titular_man,".0","") AS INT64) valor_equipe_titular_mandante,
  SAFE_CAST(REPLACE (valor_equipe_titular_vis,".0","") AS INT64) valor_equipe_titular_visitante,
  SAFE_CAST(idade_media_titular_man AS FLOAT64) idade_media_titular_mandante,
  SAFE_CAST(idade_media_titular_vis AS FLOAT64) idade_media_titular_visitante,
  SAFE_CAST(REPLACE (gols_man,".0","") AS INT64) gols_mandante,
  SAFE_CAST(REPLACE (gols_vis,".0","") AS INT64) gols_visitante,
  SAFE_CAST(REPLACE (gols_1_tempo_man,".0","") AS INT64) gols_1_tempo_mandante,
  SAFE_CAST(REPLACE (gols_1_tempo_vis,".0","") AS INT64) gols_1_tempo_visitante,
  SAFE_CAST(REPLACE (penalti,".0","") AS INT64) penalti,
  SAFE_CAST(REPLACE (gols_penalti_man,".0","") AS INT64) gols_penalti_mandante,
  SAFE_CAST(REPLACE (gols_penalti_vis,".0","") AS INT64) gols_penalti_visitante,
  SAFE_CAST(REPLACE (escanteios_man,".0","") AS INT64) escanteios_mandante,
  SAFE_CAST(REPLACE (escanteios_vis,".0","") AS INT64) escanteios_visitante,
  SAFE_CAST(REPLACE (faltas_man,".0","") AS INT64) faltas_mandante,
  SAFE_CAST(REPLACE (faltas_vis,".0","") AS INT64) faltas_visitante,
  SAFE_CAST(REPLACE (chutes_bola_parada_man,".0","") AS INT64) chutes_bola_parada_mandante,
  SAFE_CAST(REPLACE (chutes_bola_parada_vis,".0","") AS INT64) chutes_bola_parada_visitante,
  SAFE_CAST(REPLACE (defesas_man,".0","") AS INT64) defesas_mandante,
  SAFE_CAST(REPLACE (defesas_vis,".0","") AS INT64) defesas_visitante,
  SAFE_CAST(REPLACE (impedimentos_man,".0","") AS INT64) impedimentos_mandante,
  SAFE_CAST(REPLACE (impedimentos_vis,".0","") AS INT64) impedimentos_visitante,
  SAFE_CAST(REPLACE (chutes_man,".0","") AS INT64) chutes_mandante,
  SAFE_CAST(REPLACE (chutes_vis,".0","") AS INT64) chutes_visitante,
  SAFE_CAST(REPLACE (chutes_fora_man,".0","") AS INT64) chutes_fora_mandante,
  SAFE_CAST(REPLACE (chutes_fora_vis,".0","") AS INT64) chutes_fora_visitante
FROM basedosdados-staging.mundo_transfermarkt_competicoes_staging.copa_brasil AS t