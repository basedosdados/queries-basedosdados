{{
    config(
        schema="test_dataset",
        materialized="table",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY bdmais_filter                     ON `basedosdados.test_dataset.test_table`                     GRANT TO ("allUsers") FILTER USING                     (ano<2023)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON `basedosdados.test_dataset.test_table`                     GRANT TO ("group:bd-pro@basedosdados.org")                     FILTER USING                     (ano>=2023)',
            'CREATE OR REPLACE ROW ACCESS POLICY laura_filter                     ON `basedosdados.test_dataset.test_table`                     GRANT TO ("user:laura.amaral@basedosdados.org")                     FILTER USING                     (ano>=2023)',
        ],
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(dado as string) dado
from {{ set_datalake_project("test_dataset_staging.test_table") }} as t
