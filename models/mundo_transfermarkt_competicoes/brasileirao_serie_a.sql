{{ 
  config(
    schema='mundo_transfermarkt_competicoes',
    materialized='table',
     partition_by={
      "field": "ano_campeonato",
      "data_type": "int64",
      "range": {
        "start": 2003,
        "end": 2023,
        "interval": 1}
    },
    labels =  {'tema': 'esporte'},
    post_hook = ['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                ON {{this}}
                GRANT TO ("allUsers")
            FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(data), week) > 6)',
          'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                ON  {{this}}
                GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                FILTER USING (EXTRACT(YEAR from data) = EXTRACT(YEAR from  CURRENT_DATE()))' ]
    )
 }}
 
SELECT 
SAFE_CAST(REPLACE (ano_campeonato,".0","") AS INT64) ano_campeonato,
SAFE_CAST(data AS DATE) data,
SAFE_CAST(REPLACE (rodada,".0","") AS INT64) rodada,
SAFE_CAST(estadio AS STRING) estadio,
SAFE_CAST(arbitro AS STRING) arbitro,
SAFE_CAST(REPLACE (publico,".0","") AS INT64) publico,
SAFE_CAST(REPLACE (publico_max,".0","") AS INT64) publico_max,
SAFE_CAST(time_man AS STRING) time_man,
SAFE_CAST(time_vis AS STRING) time_vis,
SAFE_CAST(tecnico_man AS STRING) tecnico_man,
SAFE_CAST(tecnico_vis AS STRING) tecnico_vis,
SAFE_CAST(REPLACE (colocacao_man,".0","") AS INT64) colocacao_man,
SAFE_CAST(REPLACE (colocacao_vis,".0","") AS INT64) colocacao_vis,
SAFE_CAST(REPLACE (valor_equipe_titular_man,".0","") AS INT64) valor_equipe_titular_man,
SAFE_CAST(REPLACE (valor_equipe_titular_vis,".0","") AS INT64) valor_equipe_titular_vis,
SAFE_CAST(idade_media_titular_man AS FLOAT64) idade_media_titular_man,
SAFE_CAST(idade_media_titular_vis AS FLOAT64) idade_media_titular_vis,
SAFE_CAST(REPLACE (gols_man,".0","") AS INT64) gols_man,
SAFE_CAST(REPLACE (gols_vis,".0","") AS INT64) gols_vis,
SAFE_CAST(REPLACE (gols_1_tempo_man,".0","") AS INT64) gols_1_tempo_man,
SAFE_CAST(REPLACE (gols_1_tempo_vis,".0","") AS INT64) gols_1_tempo_vis,
SAFE_CAST(REPLACE (escanteios_man,".0","") AS INT64) escanteios_man,
SAFE_CAST(REPLACE (escanteios_vis,".0","") AS INT64) escanteios_vis,
SAFE_CAST(REPLACE (faltas_man,".0","") AS INT64) faltas_man,
SAFE_CAST(REPLACE (faltas_vis,".0","") AS INT64) faltas_vis,
SAFE_CAST(REPLACE (chutes_bola_parada_man,".0","") AS INT64) chutes_bola_parada_man,
SAFE_CAST(REPLACE (chutes_bola_parada_vis,".0","") AS INT64) chutes_bola_parada_vis,
SAFE_CAST(REPLACE (defesas_man,".0","") AS INT64) defesas_man,
SAFE_CAST(REPLACE (defesas_vis,".0","") AS INT64) defesas_vis,
SAFE_CAST(REPLACE (impedimentos_man,".0","") AS INT64) impedimentos_man,
SAFE_CAST(REPLACE (impedimentos_vis,".0","") AS INT64) impedimentos_vis,
SAFE_CAST(REPLACE (chutes_man,".0","") AS INT64) chutes_man,
SAFE_CAST(REPLACE (chutes_vis,".0","") AS INT64) chutes_vis,
SAFE_CAST(REPLACE (chutes_fora_man,".0","") AS INT64) chutes_fora_man,
SAFE_CAST(REPLACE (chutes_fora_vis,".0","") AS INT64) chutes_fora_vis
FROM basedosdados-dev.mundo_transfermarkt_competicoes_staging.brasileirao_serie_a AS t