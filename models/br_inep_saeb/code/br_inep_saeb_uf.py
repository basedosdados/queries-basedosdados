import os
import basedosdados as bd
import pandas as pd
from utils import (
    RENAMES_UFS,
    get_nivel_serie_disciplina,
    get_disciplina_serie,
    convert_to_pd_dtype,
    drop_empty_lines
)

CWD = os.path.dirname(os.getcwd())

INPUT = os.path.join(CWD, "input")
OUTPUT = os.path.join(CWD, "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

ufs_saeb_latest = pd.read_excel(
    os.path.join(INPUT, "saeb_2021_brasil_estados_municipios.xlsx"),
    dtype=str,
    sheet_name="Estados",
)

ufs_saeb_latest.head()

ufs_saeb_latest = (
    ufs_saeb_latest.drop(0, axis="index")
    .rename(columns=RENAMES_UFS, errors="raise")
    .pipe(lambda df: df.loc[df["CAPITAL"] == "Total"])
    .drop(columns=["CAPITAL", "CO_UF"])
)

ufs_saeb_latest.columns.tolist()

ufs_saeb_nivel_long_fmt = pd.melt(
    ufs_saeb_latest,
    id_vars=[
        "nome_uf",
        "rede",
        "localizacao",
    ],
    value_vars=[
        col for col in ufs_saeb_latest.columns.tolist() if col.startswith("nivel")
    ],
)

ufs_saeb_media_long_fmt = pd.melt(
    ufs_saeb_latest,
    id_vars=[
        "nome_uf",
        "rede",
        "localizacao",
    ],
    value_vars=[
        col for col in ufs_saeb_latest.columns.tolist() if col.startswith("media")
    ],
)

ufs_saeb_media_long_fmt = (
    ufs_saeb_media_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(get_disciplina_serie)
    )
    .assign(
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        serie=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
    )
    .drop(columns=["parsed_variable"])
)


ufs_saeb_nivel_long_fmt = (
    ufs_saeb_nivel_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(get_nivel_serie_disciplina)
    )
    .assign(
        nivel=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
        serie=lambda df: df["parsed_variable"].apply(lambda v: v[2]),
    )
    .drop(columns=["parsed_variable"])
)

ufs_saeb_latest_output = (
    (
        ufs_saeb_nivel_long_fmt.pivot(
            index=["nome_uf", "rede", "localizacao", "disciplina", "serie"],
            columns="nivel",
            values="value",
        )
        .reset_index()
        .merge(
            ufs_saeb_media_long_fmt.rename(columns={"value": "media"}),
            left_on=["nome_uf", "rede", "localizacao", "disciplina", "serie"],
            right_on=["nome_uf", "rede", "localizacao", "disciplina", "serie"],
        )
    )
    .drop(columns=["variable"])
    .rename(columns={i: f"nivel_{i}" for i in range(0, 11)})
)


## Clean step

ufs_saeb_latest_output.head()

ufs_saeb_latest_output["serie"].unique()
ufs_saeb_latest_output["disciplina"].unique()
ufs_saeb_latest_output["localizacao"].unique()
ufs_saeb_latest_output["rede"].unique()

bd_dirs_ufs = bd.read_sql(
    "select sigla, nome from `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

ufs_saeb_latest_output = (
    # Apenas MT e LP. Não sei porque não subiram outras disciplinas
    ufs_saeb_latest_output.loc[ufs_saeb_latest_output["disciplina"].isin(["mt", "lp"])]
    .assign(
        disciplina=lambda df: df["disciplina"].str.upper(),
        rede=lambda df: df["rede"].str.lower(),
        localizacao=lambda df: df["localizacao"].str.lower(),
        # NOTE: O saeb é aplicado no ensino fundamental e médio
        # 2, 5 e 9 serie do ensino fundamental
        # O xlsx divulgado pelo INEP traz uma coluna no formato {media|nivel}_{numero}_{disciplina}
        # Exemplo: `nivel_3_MT5`, `nivel_0_LP12`, veja `RENAMES_UFS`
        #
        # As colunas para o ensino médio são essas:
        #
        # 12: Ensino Médio Tradicional
        # 13: Ensino Médio Integrado
        # 14: Ensino Médio Tradicional + Integrado
        #
        # Os dados na BD estão em uma formato sem sentido
        # A coluna serie tem 2, 5 e 9, tudo certo, mas tem 3, 12 e 13. O que é 3? 14 virou 3?
        #
        # O que aconteceu foi:
        #
        # 12 (ensino medio tradicional) virou 3
        # 14 (Ensino Médio Tradicional + Integrado) virou 13
        # 13 (Ensino Médio Integrado) virou 12
        #
        # Veja https://github.com/basedosdados/mais/blob/a5fd53d117d3f0a83d6cf87327a9b4209cf7fcdb/bases/br_inep_saeb/code/br_inep_saeb_brasil.ipynb#L1348-L1351
        #
        # A categoria 13 e 14 que virou 12 e 13 na tabela da BD
        # começou aparacer apenas em 2021. Anteiormente era apenas
        # 2, 5, 9 e 12, representado 2, 5 e 9 série do ensino fundamental
        # e 12 o ensino médio tradicional
        #
        # Veja que o resultado da query abaixo é zero porque a categoria não
        # existia para `ano < 2021`
        #
        # `select count(*) from `basedosdados.br_inep_saeb.uf` where ano < 2021 and serie in (12, 13)`
        #
        # São várias linhas com valores nulos. Deveriam ser removidas, imo.
        #
        # Vamos manter o mesmo formato no sense
        serie=lambda df: df["serie"].replace(
            {
                # em é 12, vai virar 3
                "em": "3",
                # em_integral (Ensino Medio Integrado) é 13, vai virar 12
                "em_integral": "12",
                # em_regular (Ensino Médio Tradicional + Integrado) é 14, vai virar 13
                "em_regular": "13",
            }
        ),
        sigla_uf=lambda df: df["nome_uf"].replace(
            dict([(i["nome"], i["sigla"]) for i in bd_dirs_ufs.to_dict("records")])  # type: ignore
        ),
    )
    .drop(columns=["nome_uf"])
)

# Add column ano = 2021
ufs_saeb_latest_output["ano"] = 2021

ufs_saeb_latest_output.head()

ufs_saeb_latest_output.info()

ufs_saeb_latest_output.shape

drop_empty_lines(ufs_saeb_latest_output).shape

ufs_saeb_latest_output = drop_empty_lines(ufs_saeb_latest_output)

tb = bd.Table(dataset_id="br_inep_saeb", table_id="uf")

bq_cols = tb._get_columns_from_bq(mode="prod")

assert len(bq_cols["partition_columns"]) == 0

col_dtypes = {
    col["name"]: convert_to_pd_dtype(col["type"]) for col in bq_cols["columns"]
}

# Order columns
ufs_saeb_latest_output = ufs_saeb_latest_output.astype(col_dtypes)[col_dtypes.keys()]

upstream_df = bd.read_sql(
    "select * from `basedosdados-dev.br_inep_saeb.uf` where ano <> 2021",
    billing_project_id="basedosdados-dev",
)

assert isinstance(upstream_df, pd.DataFrame)

upstream_df["serie"].unique()

upstream_df.shape
drop_empty_lines(upstream_df).shape

upstream_df = drop_empty_lines(upstream_df)

pd.concat([ufs_saeb_latest_output, upstream_df]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "uf.csv"), index=False
)

# Update table
tb.create(
    os.path.join(OUTPUT, "uf.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
