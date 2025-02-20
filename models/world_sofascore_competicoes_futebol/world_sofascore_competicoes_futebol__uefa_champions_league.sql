{{
    config(
        schema="world_sofascore_competicoes_futebol",
        alias="uefa_champions_league",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2025, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_partida as string) id_partida,
    safe_cast(data as date) data,
    safe_cast(hora as time) hora,
    safe_cast(temporada as string) temporada,
    safe_cast(rodada as string) rodada,
    safe_cast(tempo as string) tempo,
    safe_cast(time_mandante as string) time_mandante,
    safe_cast(time_visitante as string) time_visitante,
    safe_cast(gols_mandante as int64) gols_mandante,
    safe_cast(gols_visitante as int64) gols_visitante,
    safe_cast(posse_bola_mandante as int64) posse_bola_mandante,
    safe_cast(posse_bola_visitante as int64) posse_bola_visitante,
    safe_cast(gol_esperado_mandante as float64) gol_esperado_mandante,
    safe_cast(gol_esperado_visitante as float64) gol_esperado_visitante,
    safe_cast(grande_chance_mandante as float64) grande_chance_mandante,
    safe_cast(grande_chance_visitante as float64) grande_chance_visitante,
    safe_cast(defesa_goleiro_mandante as int64) defesa_goleiro_mandante,
    safe_cast(defesa_goleiro_visitante as int64) defesa_goleiro_visitante,
    safe_cast(escanteio_mandante as int64) escanteio_mandante,
    safe_cast(escanteio_visitante as int64) escanteio_visitante,
    safe_cast(passe_mandante as int64) passe_mandante,
    safe_cast(passe_visitante as int64) passe_visitante,
    safe_cast(falta_mandante as int64) falta_mandante,
    safe_cast(falta_visitante as int64) falta_visitante,
    safe_cast(cartao_amarelo_mandante as int64) cartao_amarelo_mandante,
    safe_cast(cartao_amarelo_visitante as int64) cartao_amarelo_visitante,
    safe_cast(cartao_vermelho_mandante as int64) cartao_vermelho_mandante,
    safe_cast(cartao_vermelho_visitante as int64) cartao_vermelho_visitante,
    safe_cast(finalizacao_mandante as int64) finalizacao_mandante,
    safe_cast(finalizacao_visitante as int64) finalizacao_visitante,
    safe_cast(finalizacao_gol_mandante as int64) finalizacao_gol_mandante,
    safe_cast(finalizacao_gol_visitante as int64) finalizacao_gol_visitante,
    safe_cast(finalizacao_trave_mandante as int64) finalizacao_trave_mandante,
    safe_cast(finalizacao_trave_visitante as int64) finalizacao_trave_visitante,
    safe_cast(finalizacao_fora_mandante as int64) finalizacao_fora_mandante,
    safe_cast(finalizacao_fora_visitante as int64) finalizacao_fora_visitante,
    safe_cast(chute_bloqueado_mandante as int64) chute_bloqueado_mandante,
    safe_cast(chute_bloqueado_visitante as int64) chute_bloqueado_visitante,
    safe_cast(finalizacao_area_mandante as int64) finalizacao_area_mandante,
    safe_cast(finalizacao_area_visitante as int64) finalizacao_area_visitante,
    safe_cast(finalizacao_fora_area_mandante as int64) finalizacao_fora_area_mandante,
    safe_cast(finalizacao_fora_area_visitante as int64) finalizacao_fora_area_visitante,
    safe_cast(grande_chance_marcada_mandante as int64) grande_chance_marcada_mandante,
    safe_cast(grande_chance_marcada_visitante as int64) grande_chance_marcada_visitante,
    safe_cast(grande_chance_perdida_mandante as int64) grande_chance_perdida_mandante,
    safe_cast(grande_chance_perdida_visitante as int64) grande_chance_perdida_visitante,
    safe_cast(passe_profundidade_mandante as int64) passe_profundidade_mandante,
    safe_cast(passe_profundidade_visitante as int64) passe_profundidade_visitante,
    safe_cast(falta_terco_final_mandante as int64) falta_terco_final_mandante,
    safe_cast(falta_terco_final_visitante as int64) falta_terco_final_visitante,
    safe_cast(impedimento_mandante as int64) impedimento_mandante,
    safe_cast(impedimento_visitante as int64) impedimento_visitante,
    safe_cast(passes_certo_mandante as int64) passes_certo_mandante,
    safe_cast(passes_certo_visitante as int64) passes_certo_visitante,
    safe_cast(lateral_mandante as int64) lateral_mandante,
    safe_cast(lateral_visitante as int64) lateral_visitante,
    safe_cast(entrada_terco_final_mandante as int64) entrada_terco_final_mandante,
    safe_cast(entrada_terco_final_visitante as int64) entrada_terco_final_visitante,
    safe_cast(bola_longa_mandante as int64) bola_longa_mandante,
    safe_cast(bola_longa_visitante as int64) bola_longa_visitante,
    safe_cast(cruzamento_mandante as int64) cruzamento_mandante,
    safe_cast(cruzamento_visitante as int64) cruzamento_visitante,
    safe_cast(duelo_vencido_mandante as int64) duelo_vencido_mandante,
    safe_cast(duelo_vencido_visitante as int64) duelo_vencido_visitante,
    safe_cast(desarme_sofrido_mandante as int64) desarme_sofrido_mandante,
    safe_cast(desarme_sofrido_visitante as int64) desarme_sofrido_visitante,
    safe_cast(duelo_chao_mandante as int64) duelo_chao_mandante,
    safe_cast(duelo_chao_visitante as int64) duelo_chao_visitante,
    safe_cast(duelo_aereo_mandante as int64) duelo_aereo_mandante,
    safe_cast(duelo_aereo_visitante as int64) duelo_aereo_visitante,
    safe_cast(drible_mandante as int64) drible_mandante,
    safe_cast(drible_visitante as int64) drible_visitante,
    safe_cast(desarme_mandante as int64) desarme_mandante,
    safe_cast(desarme_visitante as int64) desarme_visitante,
    safe_cast(desarme_ganho_mandante as int64) desarme_ganho_mandante,
    safe_cast(desarme_ganho_visitante as int64) desarme_ganho_visitante,
    safe_cast(interceptacao_mandante as int64) interceptacao_mandante,
    safe_cast(interceptacao_visitante as int64) interceptacao_visitante,
    safe_cast(recuperacao_bola_mandante as int64) recuperacao_bola_mandante,
    safe_cast(recuperacao_bola_visitante as int64) recuperacao_bola_visitante,
    safe_cast(corte_mandante as int64) corte_mandante,
    safe_cast(corte_visitante as int64) corte_visitante,
    safe_cast(tiro_meta_mandante as int64) tiro_meta_mandante,
    safe_cast(tiro_meta_visitante as int64) tiro_meta_visitante,
from
    `basedosdados-staging.world_sofascore_competicoes_futebol_staging.uefa_champions_league`
    as t
