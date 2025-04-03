{{
    config(
        alias="brasil",
        materialized="table",
        schema="br_inep_indicadores_educacionais",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2006, "end": 2023, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(localizacao as string) localizacao,
    safe_cast(rede as string) rede,
    safe_cast(atu_ei as float64) atu_ei,
    safe_cast(atu_ei_creche as float64) atu_ei_creche,
    safe_cast(atu_ei_pre_escola as float64) atu_ei_pre_escola,
    safe_cast(atu_ef as float64) atu_ef,
    safe_cast(atu_ef_anos_iniciais as float64) atu_ef_anos_iniciais,
    safe_cast(atu_ef_anos_finais as float64) atu_ef_anos_finais,
    safe_cast(atu_ef_1_ano as float64) atu_ef_1_ano,
    safe_cast(atu_ef_2_ano as float64) atu_ef_2_ano,
    safe_cast(atu_ef_3_ano as float64) atu_ef_3_ano,
    safe_cast(atu_ef_4_ano as float64) atu_ef_4_ano,
    safe_cast(atu_ef_5_ano as float64) atu_ef_5_ano,
    safe_cast(atu_ef_6_ano as float64) atu_ef_6_ano,
    safe_cast(atu_ef_7_ano as float64) atu_ef_7_ano,
    safe_cast(atu_ef_8_ano as float64) atu_ef_8_ano,
    safe_cast(atu_ef_9_ano as float64) atu_ef_9_ano,
    safe_cast(atu_ef_turmas_unif_multi_fluxo as float64) atu_ef_turmas_unif_multi_fluxo,
    safe_cast(atu_em as float64) atu_em,
    safe_cast(atu_em_1_ano as float64) atu_em_1_ano,
    safe_cast(atu_em_2_ano as float64) atu_em_2_ano,
    safe_cast(atu_em_3_ano as float64) atu_em_3_ano,
    safe_cast(atu_em_4_ano as float64) atu_em_4_ano,
    safe_cast(atu_em_nao_seriado as float64) atu_em_nao_seriado,
    safe_cast(had_ei as float64) had_ei,
    safe_cast(had_ei_creche as float64) had_ei_creche,
    safe_cast(had_ei_pre_escola as float64) had_ei_pre_escola,
    safe_cast(had_ef as float64) had_ef,
    safe_cast(had_ef_anos_iniciais as float64) had_ef_anos_iniciais,
    safe_cast(had_ef_anos_finais as float64) had_ef_anos_finais,
    safe_cast(had_ef_1_ano as float64) had_ef_1_ano,
    safe_cast(had_ef_2_ano as float64) had_ef_2_ano,
    safe_cast(had_ef_3_ano as float64) had_ef_3_ano,
    safe_cast(had_ef_4_ano as float64) had_ef_4_ano,
    safe_cast(had_ef_5_ano as float64) had_ef_5_ano,
    safe_cast(had_ef_6_ano as float64) had_ef_6_ano,
    safe_cast(had_ef_7_ano as float64) had_ef_7_ano,
    safe_cast(had_ef_8_ano as float64) had_ef_8_ano,
    safe_cast(had_ef_9_ano as float64) had_ef_9_ano,
    safe_cast(had_em as float64) had_em,
    safe_cast(had_em_1_ano as float64) had_em_1_ano,
    safe_cast(had_em_2_ano as float64) had_em_2_ano,
    safe_cast(had_em_3_ano as float64) had_em_3_ano,
    safe_cast(had_em_4_ano as float64) had_em_4_ano,
    safe_cast(had_em_nao_seriado as float64) had_em_nao_seriado,
    safe_cast(tdi_ef as float64) tdi_ef,
    safe_cast(tdi_ef_anos_iniciais as float64) tdi_ef_anos_iniciais,
    safe_cast(tdi_ef_anos_finais as float64) tdi_ef_anos_finais,
    safe_cast(tdi_ef_1_ano as float64) tdi_ef_1_ano,
    safe_cast(tdi_ef_2_ano as float64) tdi_ef_2_ano,
    safe_cast(tdi_ef_3_ano as float64) tdi_ef_3_ano,
    safe_cast(tdi_ef_4_ano as float64) tdi_ef_4_ano,
    safe_cast(tdi_ef_5_ano as float64) tdi_ef_5_ano,
    safe_cast(tdi_ef_6_ano as float64) tdi_ef_6_ano,
    safe_cast(tdi_ef_7_ano as float64) tdi_ef_7_ano,
    safe_cast(tdi_ef_8_ano as float64) tdi_ef_8_ano,
    safe_cast(tdi_ef_9_ano as float64) tdi_ef_9_ano,
    safe_cast(tdi_em as float64) tdi_em,
    safe_cast(tdi_em_1_ano as float64) tdi_em_1_ano,
    safe_cast(tdi_em_2_ano as float64) tdi_em_2_ano,
    safe_cast(tdi_em_3_ano as float64) tdi_em_3_ano,
    safe_cast(tdi_em_4_ano as float64) tdi_em_4_ano,
    safe_cast(taxa_aprovacao_ef as float64) taxa_aprovacao_ef,
    safe_cast(
        taxa_aprovacao_ef_anos_iniciais as float64
    ) taxa_aprovacao_ef_anos_iniciais,
    safe_cast(taxa_aprovacao_ef_anos_finais as float64) taxa_aprovacao_ef_anos_finais,
    safe_cast(taxa_aprovacao_ef_1_ano as float64) taxa_aprovacao_ef_1_ano,
    safe_cast(taxa_aprovacao_ef_2_ano as float64) taxa_aprovacao_ef_2_ano,
    safe_cast(taxa_aprovacao_ef_3_ano as float64) taxa_aprovacao_ef_3_ano,
    safe_cast(taxa_aprovacao_ef_4_ano as float64) taxa_aprovacao_ef_4_ano,
    safe_cast(taxa_aprovacao_ef_5_ano as float64) taxa_aprovacao_ef_5_ano,
    safe_cast(taxa_aprovacao_ef_6_ano as float64) taxa_aprovacao_ef_6_ano,
    safe_cast(taxa_aprovacao_ef_7_ano as float64) taxa_aprovacao_ef_7_ano,
    safe_cast(taxa_aprovacao_ef_8_ano as float64) taxa_aprovacao_ef_8_ano,
    safe_cast(taxa_aprovacao_ef_9_ano as float64) taxa_aprovacao_ef_9_ano,
    safe_cast(taxa_aprovacao_em as float64) taxa_aprovacao_em,
    safe_cast(taxa_aprovacao_em_1_ano as float64) taxa_aprovacao_em_1_ano,
    safe_cast(taxa_aprovacao_em_2_ano as float64) taxa_aprovacao_em_2_ano,
    safe_cast(taxa_aprovacao_em_3_ano as float64) taxa_aprovacao_em_3_ano,
    safe_cast(taxa_aprovacao_em_4_ano as float64) taxa_aprovacao_em_4_ano,
    safe_cast(taxa_aprovacao_em_nao_seriado as float64) taxa_aprovacao_em_nao_seriado,
    safe_cast(taxa_reprovacao_ef as float64) taxa_reprovacao_ef,
    safe_cast(
        taxa_reprovacao_ef_anos_iniciais as float64
    ) taxa_reprovacao_ef_anos_iniciais,
    safe_cast(taxa_reprovacao_ef_anos_finais as float64) taxa_reprovacao_ef_anos_finais,
    safe_cast(taxa_reprovacao_ef_1_ano as float64) taxa_reprovacao_ef_1_ano,
    safe_cast(taxa_reprovacao_ef_2_ano as float64) taxa_reprovacao_ef_2_ano,
    safe_cast(taxa_reprovacao_ef_3_ano as float64) taxa_reprovacao_ef_3_ano,
    safe_cast(taxa_reprovacao_ef_4_ano as float64) taxa_reprovacao_ef_4_ano,
    safe_cast(taxa_reprovacao_ef_5_ano as float64) taxa_reprovacao_ef_5_ano,
    safe_cast(taxa_reprovacao_ef_6_ano as float64) taxa_reprovacao_ef_6_ano,
    safe_cast(taxa_reprovacao_ef_7_ano as float64) taxa_reprovacao_ef_7_ano,
    safe_cast(taxa_reprovacao_ef_8_ano as float64) taxa_reprovacao_ef_8_ano,
    safe_cast(taxa_reprovacao_ef_9_ano as float64) taxa_reprovacao_ef_9_ano,
    safe_cast(taxa_reprovacao_em as float64) taxa_reprovacao_em,
    safe_cast(taxa_reprovacao_em_1_ano as float64) taxa_reprovacao_em_1_ano,
    safe_cast(taxa_reprovacao_em_2_ano as float64) taxa_reprovacao_em_2_ano,
    safe_cast(taxa_reprovacao_em_3_ano as float64) taxa_reprovacao_em_3_ano,
    safe_cast(taxa_reprovacao_em_4_ano as float64) taxa_reprovacao_em_4_ano,
    safe_cast(taxa_reprovacao_em_nao_seriado as float64) taxa_reprovacao_em_nao_seriado,
    safe_cast(taxa_abandono_ef as float64) taxa_abandono_ef,
    safe_cast(taxa_abandono_ef_anos_iniciais as float64) taxa_abandono_ef_anos_iniciais,
    safe_cast(taxa_abandono_ef_anos_finais as float64) taxa_abandono_ef_anos_finais,
    safe_cast(taxa_abandono_ef_1_ano as float64) taxa_abandono_ef_1_ano,
    safe_cast(taxa_abandono_ef_2_ano as float64) taxa_abandono_ef_2_ano,
    safe_cast(taxa_abandono_ef_3_ano as float64) taxa_abandono_ef_3_ano,
    safe_cast(taxa_abandono_ef_4_ano as float64) taxa_abandono_ef_4_ano,
    safe_cast(taxa_abandono_ef_5_ano as float64) taxa_abandono_ef_5_ano,
    safe_cast(taxa_abandono_ef_6_ano as float64) taxa_abandono_ef_6_ano,
    safe_cast(taxa_abandono_ef_7_ano as float64) taxa_abandono_ef_7_ano,
    safe_cast(taxa_abandono_ef_8_ano as float64) taxa_abandono_ef_8_ano,
    safe_cast(taxa_abandono_ef_9_ano as float64) taxa_abandono_ef_9_ano,
    safe_cast(taxa_abandono_em as float64) taxa_abandono_em,
    safe_cast(taxa_abandono_em_1_ano as float64) taxa_abandono_em_1_ano,
    safe_cast(taxa_abandono_em_2_ano as float64) taxa_abandono_em_2_ano,
    safe_cast(taxa_abandono_em_3_ano as float64) taxa_abandono_em_3_ano,
    safe_cast(taxa_abandono_em_4_ano as float64) taxa_abandono_em_4_ano,
    safe_cast(taxa_abandono_em_nao_seriado as float64) taxa_abandono_em_nao_seriado,
    safe_cast(tnr_ef as float64) tnr_ef,
    safe_cast(tnr_ef_anos_iniciais as float64) tnr_ef_anos_iniciais,
    safe_cast(tnr_ef_anos_finais as float64) tnr_ef_anos_finais,
    safe_cast(tnr_ef_1_ano as float64) tnr_ef_1_ano,
    safe_cast(tnr_ef_2_ano as float64) tnr_ef_2_ano,
    safe_cast(tnr_ef_3_ano as float64) tnr_ef_3_ano,
    safe_cast(tnr_ef_4_ano as float64) tnr_ef_4_ano,
    safe_cast(tnr_ef_5_ano as float64) tnr_ef_5_ano,
    safe_cast(tnr_ef_6_ano as float64) tnr_ef_6_ano,
    safe_cast(tnr_ef_7_ano as float64) tnr_ef_7_ano,
    safe_cast(tnr_ef_8_ano as float64) tnr_ef_8_ano,
    safe_cast(tnr_ef_9_ano as float64) tnr_ef_9_ano,
    safe_cast(tnr_em as float64) tnr_em,
    safe_cast(tnr_em_1_ano as float64) tnr_em_1_ano,
    safe_cast(tnr_em_2_ano as float64) tnr_em_2_ano,
    safe_cast(tnr_em_3_ano as float64) tnr_em_3_ano,
    safe_cast(tnr_em_4_ano as float64) tnr_em_4_ano,
    safe_cast(tnr_em_nao_seriado as float64) tnr_em_nao_seriado,
    safe_cast(dsu_ei as float64) dsu_ei,
    safe_cast(dsu_ei_creche as float64) dsu_ei_creche,
    safe_cast(dsu_ei_pre_escola as float64) dsu_ei_pre_escola,
    safe_cast(dsu_ef as float64) dsu_ef,
    safe_cast(dsu_ef_anos_iniciais as float64) dsu_ef_anos_iniciais,
    safe_cast(dsu_ef_anos_finais as float64) dsu_ef_anos_finais,
    safe_cast(dsu_em as float64) dsu_em,
    safe_cast(dsu_ep as float64) dsu_ep,
    safe_cast(dsu_eja as float64) dsu_eja,
    safe_cast(dsu_ee as float64) dsu_ee,
    safe_cast(afd_ei_grupo_1 as float64) afd_ei_grupo_1,
    safe_cast(afd_ei_grupo_2 as float64) afd_ei_grupo_2,
    safe_cast(afd_ei_grupo_3 as float64) afd_ei_grupo_3,
    safe_cast(afd_ei_grupo_4 as float64) afd_ei_grupo_4,
    safe_cast(afd_ei_grupo_5 as float64) afd_ei_grupo_5,
    safe_cast(afd_ef_grupo_1 as float64) afd_ef_grupo_1,
    safe_cast(afd_ef_grupo_2 as float64) afd_ef_grupo_2,
    safe_cast(afd_ef_grupo_3 as float64) afd_ef_grupo_3,
    safe_cast(afd_ef_grupo_4 as float64) afd_ef_grupo_4,
    safe_cast(afd_ef_grupo_5 as float64) afd_ef_grupo_5,
    safe_cast(afd_ef_anos_iniciais_grupo_1 as float64) afd_ef_anos_iniciais_grupo_1,
    safe_cast(afd_ef_anos_iniciais_grupo_2 as float64) afd_ef_anos_iniciais_grupo_2,
    safe_cast(afd_ef_anos_iniciais_grupo_3 as float64) afd_ef_anos_iniciais_grupo_3,
    safe_cast(afd_ef_anos_iniciais_grupo_4 as float64) afd_ef_anos_iniciais_grupo_4,
    safe_cast(afd_ef_anos_iniciais_grupo_5 as float64) afd_ef_anos_iniciais_grupo_5,
    safe_cast(afd_ef_anos_finais_grupo_1 as float64) afd_ef_anos_finais_grupo_1,
    safe_cast(afd_ef_anos_finais_grupo_2 as float64) afd_ef_anos_finais_grupo_2,
    safe_cast(afd_ef_anos_finais_grupo_3 as float64) afd_ef_anos_finais_grupo_3,
    safe_cast(afd_ef_anos_finais_grupo_4 as float64) afd_ef_anos_finais_grupo_4,
    safe_cast(afd_ef_anos_finais_grupo_5 as float64) afd_ef_anos_finais_grupo_5,
    safe_cast(afd_em_grupo_1 as float64) afd_em_grupo_1,
    safe_cast(afd_em_grupo_2 as float64) afd_em_grupo_2,
    safe_cast(afd_em_grupo_3 as float64) afd_em_grupo_3,
    safe_cast(afd_em_grupo_4 as float64) afd_em_grupo_4,
    safe_cast(afd_em_grupo_5 as float64) afd_em_grupo_5,
    safe_cast(afd_eja_fundamental_grupo_1 as float64) afd_eja_fundamental_grupo_1,
    safe_cast(afd_eja_fundamental_grupo_2 as float64) afd_eja_fundamental_grupo_2,
    safe_cast(afd_eja_fundamental_grupo_3 as float64) afd_eja_fundamental_grupo_3,
    safe_cast(afd_eja_fundamental_grupo_4 as float64) afd_eja_fundamental_grupo_4,
    safe_cast(afd_eja_fundamental_grupo_5 as float64) afd_eja_fundamental_grupo_5,
    safe_cast(afd_eja_medio_grupo_1 as float64) afd_eja_medio_grupo_1,
    safe_cast(afd_eja_medio_grupo_2 as float64) afd_eja_medio_grupo_2,
    safe_cast(afd_eja_medio_grupo_3 as float64) afd_eja_medio_grupo_3,
    safe_cast(afd_eja_medio_grupo_4 as float64) afd_eja_medio_grupo_4,
    safe_cast(afd_eja_medio_grupo_5 as float64) afd_eja_medio_grupo_5,
    safe_cast(ird_baixa_regularidade as float64) ird_baixa_regularidade,
    safe_cast(ird_media_baixa as float64) ird_media_baixa,
    safe_cast(ird_media_alta as float64) ird_media_alta,
    safe_cast(ird_alta as float64) ird_alta,
    safe_cast(ied_ef_nivel_1 as float64) ied_ef_nivel_1,
    safe_cast(ied_ef_nivel_2 as float64) ied_ef_nivel_2,
    safe_cast(ied_ef_nivel_3 as float64) ied_ef_nivel_3,
    safe_cast(ied_ef_nivel_4 as float64) ied_ef_nivel_4,
    safe_cast(ied_ef_nivel_5 as float64) ied_ef_nivel_5,
    safe_cast(ied_ef_nivel_6 as float64) ied_ef_nivel_6,
    safe_cast(ied_ef_anos_iniciais_nivel_1 as float64) ied_ef_anos_iniciais_nivel_1,
    safe_cast(ied_ef_anos_iniciais_nivel_2 as float64) ied_ef_anos_iniciais_nivel_2,
    safe_cast(ied_ef_anos_iniciais_nivel_3 as float64) ied_ef_anos_iniciais_nivel_3,
    safe_cast(ied_ef_anos_iniciais_nivel_4 as float64) ied_ef_anos_iniciais_nivel_4,
    safe_cast(ied_ef_anos_iniciais_nivel_5 as float64) ied_ef_anos_iniciais_nivel_5,
    safe_cast(ied_ef_anos_iniciais_nivel_6 as float64) ied_ef_anos_iniciais_nivel_6,
    safe_cast(ied_ef_anos_finais_nivel_1 as float64) ied_ef_anos_finais_nivel_1,
    safe_cast(ied_ef_anos_finais_nivel_2 as float64) ied_ef_anos_finais_nivel_2,
    safe_cast(ied_ef_anos_finais_nivel_3 as float64) ied_ef_anos_finais_nivel_3,
    safe_cast(ied_ef_anos_finais_nivel_4 as float64) ied_ef_anos_finais_nivel_4,
    safe_cast(ied_ef_anos_finais_nivel_5 as float64) ied_ef_anos_finais_nivel_5,
    safe_cast(ied_ef_anos_finais_nivel_6 as float64) ied_ef_anos_finais_nivel_6,
    safe_cast(ied_em_nivel_1 as float64) ied_em_nivel_1,
    safe_cast(ied_em_nivel_2 as float64) ied_em_nivel_2,
    safe_cast(ied_em_nivel_3 as float64) ied_em_nivel_3,
    safe_cast(ied_em_nivel_4 as float64) ied_em_nivel_4,
    safe_cast(ied_em_nivel_5 as float64) ied_em_nivel_5,
    safe_cast(ied_em_nivel_6 as float64) ied_em_nivel_6,
    safe_cast(icg_nivel_1 as float64) icg_nivel_1,
    safe_cast(icg_nivel_2 as float64) icg_nivel_2,
    safe_cast(icg_nivel_3 as float64) icg_nivel_3,
    safe_cast(icg_nivel_4 as float64) icg_nivel_4,
    safe_cast(icg_nivel_5 as float64) icg_nivel_5,
    safe_cast(icg_nivel_6 as float64) icg_nivel_6,
from {{ set_datalake_project("br_inep_indicadores_educacionais_staging.brasil") }} as t
