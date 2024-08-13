{{
    config(
        schema="br_rf_arrecadacao",
        alias="uf",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(imposto_importacao as float64) imposto_importacao,
    safe_cast(imposto_exportacao as float64) imposto_exportacao,
    safe_cast(ipi_fumo as float64) ipi_fumo,
    safe_cast(ipi_bebidas as float64) ipi_bebidas,
    safe_cast(ipi_automoveis as float64) ipi_automoveis,
    safe_cast(ipi_importacoes as float64) ipi_importacoes,
    safe_cast(ipi_outros as float64) ipi_outros,
    safe_cast(irpf as float64) irpf,
    safe_cast(irpj_entidades_financeiras as float64) irpj_entidades_financeiras,
    safe_cast(irpj_demais_empresas as float64) irpj_demais_empresas,
    safe_cast(irrf_rendimentos_trabalho as float64) irrf_rendimentos_trabalho,
    safe_cast(irrf_rendimentos_capital as float64) irrf_rendimentos_capital,
    safe_cast(irrf_remessas_exterior as float64) irrf_remessas_exterior,
    safe_cast(irrf_outros_rendimentos as float64) irrf_outros_rendimentos,
    safe_cast(iof as float64) iof,
    safe_cast(itr as float64) itr,
    safe_cast(ipmf as float64) ipmf,
    safe_cast(cpmf as float64) cpmf,
    safe_cast(cofins as float64) cofins,
    safe_cast(cofins_financeiras as float64) cofins_entidades_financeiras,
    safe_cast(cofins_demais_empresas as float64) cofins_demais_empresas,
    safe_cast(pis_pasep as float64) pis_pasep,
    safe_cast(
        pis_pasep_entidades_financeiras as float64
    ) pis_pasep_entidades_financeiras,
    safe_cast(pis_pasep_demais_empresas as float64) pis_pasep_demais_empresas,
    safe_cast(csll as float64) csll,
    safe_cast(csll_financeiras as float64) csll_entidades_financeiras,
    safe_cast(csll_demais_empresas as float64) csll_demais_empresas,
    safe_cast(
        cide_combustiveis_parcela_nao_dedutivel as float64
    ) cide_combustiveis_parcela_nao_dedutivel,
    safe_cast(cide_combustiveis as float64) cide_combustiveis,
    safe_cast(cpsss_1 as float64) cpsss_1,
    safe_cast(cpsss_2 as float64) cpsss_2,
    safe_cast(contribuicoes_fundaf as float64) contribuicao_fundaf,
    safe_cast(refis as float64) refis,
    safe_cast(paes as float64) paes,
    safe_cast(retencoes_fonte as float64) retencoes_fonte,
    safe_cast(pagamento_unificado as float64) pagamento_unificado,
    safe_cast(outras_receitas_ as float64) outras_receitas_rfb,
    safe_cast(demais_receitas as float64) demais_receitas,
    safe_cast(receita_previdenciaria as float64) receita_previdenciaria,
    safe_cast(receita_previdenciaria_propria as float64) receita_previdenciaria_propria,
    safe_cast(receita_previdenciaria_demais as float64) receita_previdenciaria_demais,
    safe_cast(receitas_outros_orgaos as float64) receitas_outros_orgaos,
from `basedosdados-staging.br_rf_arrecadacao_staging.uf` as t
