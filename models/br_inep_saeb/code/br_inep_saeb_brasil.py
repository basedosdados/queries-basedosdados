import os
import basedosdados as bd
import pandas as pd
from utils import (
    RENAMES_BR,
    get_nivel_serie_disciplina,
    get_disciplina_serie,
    convert_to_pd_dtype,
)

CWD = os.path.dirname(os.getcwd())

INPUT = os.path.join(CWD, "input")
OUTPUT = os.path.join(CWD, "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

br_saeb_latest = pd.read_excel(
    os.path.join(INPUT, "saeb_2021_brasil_estados_municipios.xlsx"),
    dtype=str,
    sheet_name="Brasil",
)

br_saeb_latest.head()

br_saeb_latest = (
    br_saeb_latest.drop(0, axis="index")
    .rename(columns=RENAMES_BR, errors="raise")
    .pipe(lambda df: df.loc[df["CAPITAL"] == "Total"])
    .drop(columns=["CAPITAL", "ID"])
)

br_saeb_latest.columns.tolist()

br_saeb_nivel_long_fmt = pd.melt(
    br_saeb_latest,
    id_vars=[
        "rede",
        "localizacao",
    ],
    value_vars=[
        col for col in br_saeb_latest.columns.tolist() if col.startswith("nivel")
    ],
)

br_saeb_media_long_fmt = pd.melt(
    br_saeb_latest,
    id_vars=[
        "rede",
        "localizacao",
    ],
    value_vars=[
        col for col in br_saeb_latest.columns.tolist() if col.startswith("media")
    ],
)


br_saeb_media_long_fmt = (
    br_saeb_media_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(get_disciplina_serie)
    )
    .assign(
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        serie=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
    )
    .drop(columns=["parsed_variable"])
)


br_saeb_nivel_long_fmt = (
    br_saeb_nivel_long_fmt.assign(
        parsed_variable=lambda df: df["variable"].apply(get_nivel_serie_disciplina)
    )
    .assign(
        nivel=lambda df: df["parsed_variable"].apply(lambda v: v[0]),
        disciplina=lambda df: df["parsed_variable"].apply(lambda v: v[1]),
        serie=lambda df: df["parsed_variable"].apply(lambda v: v[2]),
    )
    .drop(columns=["parsed_variable"])
)

br_saeb_latest_output = (
    (
        br_saeb_nivel_long_fmt.pivot(
            index=["rede", "localizacao", "disciplina", "serie"],
            columns="nivel",
            values="value",
        )
        .reset_index()
        .merge(
            br_saeb_media_long_fmt.rename(columns={"value": "media"}),
            left_on=["rede", "localizacao", "disciplina", "serie"],
            right_on=["rede", "localizacao", "disciplina", "serie"],
        )
    )
    .drop(columns=["variable"])
    .rename(columns={i: f"nivel_{i}" for i in range(0, 11)})
)


## Clean step

br_saeb_latest_output.head()

br_saeb_latest_output["serie"].unique()
br_saeb_latest_output["disciplina"].unique()
br_saeb_latest_output["localizacao"].unique()
br_saeb_latest_output["rede"].unique()

br_saeb_latest_output = (
    # apenas MT e LP
    br_saeb_latest_output.loc[br_saeb_latest_output["disciplina"].isin(["mt", "lp"])]
    .pipe(
        # vamos remover em_regular (Ensino Médio Integrado)
        lambda df: df.loc[df["serie"] != "em_regular"]
    )
    .assign(
        disciplina=lambda df: df["disciplina"].str.upper(),
        rede=lambda df: df["rede"].str.lower(),
        localizacao=lambda df: df["localizacao"].str.lower(),
        serie=lambda df: df["serie"].replace({"em": "3", "em_integral": "4"}),
    )
)

br_saeb_latest_output["ano"] = 2021

br_saeb_latest_output.head()

br_saeb_latest_output.info()

tb = bd.Table(dataset_id="br_inep_saeb", table_id="brasil")

bq_cols = tb._get_columns_from_bq(mode="prod")

assert len(bq_cols["partition_columns"]) == 0

col_dtypes = {
    col["name"]: convert_to_pd_dtype(col["type"]) for col in bq_cols["columns"]
}

# Order columns
br_saeb_latest_output = br_saeb_latest_output.astype(col_dtypes)[col_dtypes.keys()]

upstream_df = bd.read_sql(
    "select * from `basedosdados.br_inep_saeb.brasil`",
    billing_project_id="basedosdados-dev",
)

pd.concat([br_saeb_latest_output, upstream_df]).to_csv(  # type: ignore
    os.path.join(OUTPUT, "brasil.csv"), index=False
)
