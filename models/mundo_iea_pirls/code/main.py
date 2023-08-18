# %%
import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import basedosdados as bd

from pirls_utils import LABELS_FROM_CONTEXT_QUESTIONNAIRES

# %%
# CWD = os.path.join(os.getcwd(), "models", "mundo_iea_pirls") if __name__ == "__main__" else os.path.dirname(os.getcwd())

# CWD = os.path.join(os.getcwd(), "models", "mundo_iea_pirls")
CWD = os.path.dirname(os.getcwd())
INPUT = os.path.join(CWD, "input")
TMP = os.path.join(CWD, "tmp")
OUTPUT = os.path.join(CWD, "output")

# %%
PIRLS_ASSETS_URLS = {
    "data": "https://pirls2021.org/data/downloads/P21_Data_SAS.zip",
    "item_information": "https://pirls2021.org/data/downloads/P21_ItemInformation.xlsx",
    "codebooks": "https://pirls2021.org/data/downloads/P21_Codebooks.zip",
}

# %%
if not os.path.exists(INPUT):
    os.mkdir(INPUT)

if not os.path.exists(TMP):
    os.mkdir(TMP)

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

# r = requests.get(PIRLS_ASSETS_URLS["data"])
# z = zipfile.ZipFile(io.BytesIO(r.content))
# z.extractall(INPUT)

# r = requests.get(PIRLS_ASSETS_URLS["codebooks"])
# z = zipfile.ZipFile(io.BytesIO(r.content))
# z.extractall(INPUT)

# r = requests.get(PIRLS_ASSETS_URLS["item_information"], stream=True)
# with open(f"{INPUT}/item_information.xlsx", "wb") as file:
#     file.write(r.content)

# for file in os.listdir(f"{INPUT}/Data"):
#     parquet_filename = file.replace(".sas7bdat", ".parquet")
#     parquet_output_filename = f"{TMP}/data/{parquet_filename}"

#     pd.read_sas(f"{INPUT}/Data/{file}").to_parquet(parquet_output_filename)

PIRLS_TABLES_DESC = {
    "acg": "school_context",
    "asa": "student_achievement",
    "asg": "student_context",
    "ash": "home_context",
    "asr": "within_country_scoring_reliability",
    "ast": "student_teacher_link",
    "atg": "teacher_context",
}

TABLES_TO_PROCESS = list(PIRLS_TABLES_DESC.keys())[6:7]
print(TABLES_TO_PROCESS)


# %%
def read_pirls_files(tables: list[str]):
    files = os.listdir(f"{TMP}/data")

    # result = dict(acg=[], asa=[], asg=[], ash=[], asr=[], ast=[], atg=[])
    result = {k: [] for k in tables}

    for file in files:
        is_normal_pirls = file.endswith("r5.parquet")
        table = file[0:3]
        df = pd.read_parquet(f"{TMP}/data/{file}")

        df["pirls_type"] = "Normal" if is_normal_pirls else "Bridge"

        if table in result:
            result[table].append(df)

    return {
        table: pd.concat(result[table], ignore_index=True) for table in result.keys()
    }


# %%

# dfs = read_pirls_files(TABLES_TO_PROCESS)

# %% [markdown]
# # Gerar arquitetura

# %%
RENAMES = {
    "asa": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "WAVE": "student_wave_indicator",
        "IDSCHOOL": "school_id",
        "IDCLASS": "class_id",
        "IDSTUD": "student_id",
        "ITSEX": "sex_student",
        "ITADMINI": "test_administrator_position",
        "ITLANG_SA": "language_student_achievement_test",
        "LCID_SA": "locale_student_test_id",
        "IDBOOK": "booklet_id",
        "ITDEV": "administration_device",
        "ASDAGE": "student_age",
        "ILRELIAB": "reliability_coding_status",
        "HOUWGT": "house_weight",
        "TOTWGT": "total_student_weight",
        "SENWGT": "senate_weight",
        "WGTADJ1": "school_weight_adjustment",
        "WGTADJ2": "class_weight_adjustment",
        "WGTADJ3": "student_weight_adjustment",
        "WGTFAC1": "school_weight_factor",
        "WGTFAC2": "class_weight_factor",
        "WGTFAC3": "student_weight_factor",
        "JKREP": "jackknife_replicate_code",
        "JKZONE": "jackknife_zone",
        "SCOPE": "scope",
        "VERSION": "version",
    },
    "asr": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "WAVE": "student_wave_indicator",
        "IDSCHOOL": "school_id",
        "IDCLASS": "class_id",
        "IDSTUD": "student_id",
        "IDBOOK": "booklet_id",
        "SCOPE": "scope",
        "VERSION": "version",
    },
    "asg": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "WAVE": "student_wave_indicator",
        "IDSCHOOL": "school_id",
        "IDCLASS": "class_id",
        "IDSTUD": "student_id",
        "ITSEX": "sex_student",
        "ITADMINI": "test_administrator_position",
        "LCID_SA": "locale_student_test_id",
        "ITLANG_SA": "language_student_achievement_test",
        "ITLANG_SQ": "language_student_achievement_questionnaire",
        "LCID_SQ": "locale_student_questionnaire_id",
        "IDBOOK": "booklet_id",
        "SCOPE": "scope",
        "VERSION": "version",
    },
    "ash": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "WAVE": "student_wave_indicator",
        "IDSCHOOL": "school_id",
        "IDCLASS": "class_id",
        "IDSTUD": "student_id",
        "ITLANG_HQ": "language_home_questionnaire",
        "LCID_HQ": "locale_student_home_questionnaire_id",
        "SCOPE": "scope",
        "VERSION": "version",
    },
    "acg": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "IDSCHOOL": "school_id",
        "ITLANG_CQ": "language_school_questionnaire",
        "LCID_CQ": "locale_school_questionnaire_id",
        "SCHWGT": "school_level_weight",
        "STOTWGTU": "sum_student_weights",
        "WGTADJ1": "school_weight_adjustment",
        "WGTFAC1": "school_weight_factor",
        "JKCREP": "replicate_code",
        "JKCZONE": "zone_code",
        "SCOPE": "scope",
        "VERSION": "version",
    },
    "atg": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "IDSCHOOL": "school_id",
        "IDTEACH": "teacher_id",
        "IDLINK": "teacher_link_number",
        "IDTEALIN": "teacher_link_id",
        "ITLANG_TQ": "language_teacher_questionnaire",
        "LCID_TQ": "locale_teacher_questionnaire_id",
        # "isDummy": "is_dummy",
        "SCOPE": "scope",
        "VERSION": "version",
    },
    "ast": {
        "IDCNTRY": "country_id_iso3",
        "IDPOP": "population_id",
        "IDGRADER": "standardized_grade_id",
        "IDGRADE": "grade_id",
        "WAVE": "student_wave_indicator",
        "IDSCHOOL": "school_id",
        "IDCLASS": "class_id",
        "IDSTUD": "student_id",
        "IDTEACH": "teacher_id",
        "IDLINK": "teacher_link_number",
        "IDTEALIN": "teacher_link_id",
        "IDBOOK": "booklet_id",
        "IDSUBJ": "subject_id",
        "NTEACH": "number_teachers",
        "TCHWGT": "weight_teacher",
        "JKREP": "jackknife_replicate_code",
        "JKZONE": "jackknife_zone",
        "SCOPE": "scope",
        "VERSION": "version",
    },
}


# %%
def read_codebooks() -> dict[str, pd.DataFrame]:
    cols = [
        "Variable",
        "Label",
        "Level",
        "Value Scheme Detailed",
        "Decimals",
        "Comment",
    ]

    sheets_normal = ["ASAR5", "ASRR5", "ASGR5", "ASHR5", "ACGR5", "ATGR5", "ASTR5"]
    sheets_bridge = ["ASAA5", "ASRA5", "ASGA5", "ASHA5", "ACGA5", "ATGA5", "ASTA5"]

    restrict_use = "In restricted-use IDB only"

    def read_sheet_from_file(
        filename: str, sheet_name: str, pirls_type: str
    ) -> pd.DataFrame:
        df = pd.read_excel(f"{INPUT}/{filename}", sheet_name=sheet_name)[cols]
        df["pirls_type"] = pirls_type
        return df[df["Comment"] != restrict_use].drop(columns=["Comment"])

    dfs_normal = {
        sheet_name[0:3].lower(): read_sheet_from_file(
            "P21_Codebook.xlsx", sheet_name, "Normal"
        )
        for sheet_name in sheets_normal
    }
    dfs_bridge = {
        sheet_name[0:3].lower(): read_sheet_from_file(
            "P21Br_Codebook.xlsx", sheet_name, "Bridge"
        )
        for sheet_name in sheets_bridge
    }

    def read_sheets(table_prefix: str) -> pd.DataFrame:
        sheet_name_normal = f"{table_prefix.upper()}R5"
        sheet_name_bridge = f"{table_prefix.upper()}A5"
        normal = pd.read_excel(
            f"{INPUT}/P21_Codebook.xlsx", sheet_name=sheet_name_normal
        )[cols]
        bridge = pd.read_excel(
            f"{INPUT}/P21Br_Codebook.xlsx", sheet_name=sheet_name_bridge
        )[cols]
        normal["pirls_type"] = "Normal"
        bridge["pirls_type"] = "Bridge"
        df = pd.concat([normal, bridge])
        return (
            df[df["Comment"] != restrict_use]
            .drop(columns=["Comment"])
            .drop_duplicates(subset=["Variable"])
        )

    result = {k: read_sheets(k) for k in PIRLS_TABLES_DESC.keys()}

    return result
    # return {"normal": dfs_normal, "bridge": dfs_bridge}


# %%
def get_item_informations() -> pd.DataFrame:
    sheets = ["P21 Paper", "P21 Digital", "P21Br (Paper)"]
    cols = ["Item ID", "Label"]

    dfs: list[pd.DataFrame] = [
        pd.read_excel(f"{INPUT}/item_information.xlsx", sheet_name=sheet_name)[cols]
        for sheet_name in sheets
    ]

    result = pd.concat(dfs).drop_duplicates(subset=["Item ID"])
    assert result is not None
    return result


# %%
codebooks = read_codebooks()

# %%
item_informations = get_item_informations()


# %%
def gen_archs(codebooks: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    def find_label(table, variable, label_original, label_from_item):
        if pd.isnull(label_from_item):
            if (
                table in LABELS_FROM_CONTEXT_QUESTIONNAIRES
                and variable in LABELS_FROM_CONTEXT_QUESTIONNAIRES[table]
            ):
                return LABELS_FROM_CONTEXT_QUESTIONNAIRES[table][variable]
            else:
                return label_original
        else:
            return label_from_item

    def get_bigquery_type(variable, level, decimals, scheme):
        if variable in [
            "IDPOP",
            "IDGRADER",
            "IDGRADE",
            "WAVE",
            "IDSCHOOL",
            "IDCLASS",
            "IDSTUD",
            "ITSEX",
            "ITADMINI",
            "ITLANG_SA",
            "LCID_SA",
            "IDBOOK",
            "ITDEV",
            "VERSION",
            "SCOPE",
        ]:
            return "string"

        if not pd.isnull(scheme) and scheme.strip() == "1: Yes; 2: No":
            return "bool"

        if decimals > 0:
            assert pd.isnull(scheme)
            return "float64"
        else:
            return "int64" if level == "Scale" else "string"

    def process_table(df: pd.DataFrame, table: str):
        order = [
            "name",
            "bigquery_type",
            "description",
            "temporal_coverage",
            "covered_by_dictionary",
            "directory_column",
            "measurement_unit",
            "has_sensitive_data",
            "observations",
            "original_name",
        ]

        df["name"] = df["Variable"].apply(
            lambda var: RENAMES[table][var] if var in RENAMES[table] else var.lower()
        )
        df = df.merge(
            item_informations, how="left", left_on="Variable", right_on="Item ID"
        )

        df["description"] = df[["Variable", "Label_x", "Label_y"]].apply(
            lambda cols: find_label(table, cols[0], cols[1], cols[2]).title(), axis=1
        )

        df["bigquery_type"] = df[
            ["Variable", "Level", "Decimals", "Value Scheme Detailed"]
        ].apply(lambda cols: get_bigquery_type(*cols), axis=1)

        df["covered_by_dictionary"] = df[
            ["bigquery_type", "Value Scheme Detailed"]
        ].apply(
            lambda cols: "no" if cols[0] == "bool" or pd.isnull(cols[1]) else "yes",
            axis=1,
        )

        df["observations"] = None

        df["directory_column"] = df["Variable"].apply(
            lambda var: "br_bd_diretorios_mundo.pais:id_pais_m49"
            if var == "IDCNTRY"
            else None
        )

        df["measurement_unit"] = df["Variable"].apply(
            lambda var: "person" if var == "NTEACH" else None
        )

        df["temporal_coverage"] = None
        df["has_sensitive_data"] = "no"

        df = df[df["Variable"] != "isDummy"]

        pirls_type_row = pd.DataFrame(
            [
                {
                    "name": "pirls_type",
                    "bigquery_type": "string",
                    "description": "Indicates if the record is from PIRLS Bridge or PIRLS Normal",
                    "temporal_coverage": None,
                    "covered_by_dictionary": "no",
                    "directory_column": None,
                    "measurement_unit": None,
                    "has_sensitive_data": "no",
                    "observations": None,
                    "original_name": None,
                }
            ]
        )

        df = (
            df.drop(columns=["Label_x", "Item ID", "Label_y", "Decimals"])
            .rename(columns={"Variable": "original_name"})
            .filter(order)
        )

        return pd.concat([df, pirls_type_row])

    # return process_table(codebooks["normal"]["asa"], "asa")

    result = dict(acg=[], asa=[], asg=[], ash=[], asr=[], ast=[], atg=[])

    for table in codebooks.keys():
        df = codebooks[table]

        result[table].append(process_table(df, table))

    return {
        table: pd.concat(result[table]).drop_duplicates(subset=["name"])
        for table in result.keys()
    }  # type: ignore


tables_archs = gen_archs(codebooks)

# for table in tables_archs.keys():
#     df = tables_archs[table]
#     table_name = f"{CWD}/extra/architecture/{PIRLS_TABLES_DESC[table]}_{table}.xlsx"
#     df.to_excel(table_name, index=False)


def get_types_cols(table: str) -> dict[str, str]:
    types = tables_archs[table][["original_name", "bigquery_type"]].to_dict(
        orient="split", index=False
    )["data"]
    return {key: typ for (key, typ) in types if key is not None}


def clean_data(df: pd.DataFrame, table: str):
    types_cols = {k: v for (k, v) in get_types_cols(table).items() if k in df.columns}

    cols_bool_type = [k for (k, v) in types_cols.items() if v == "bool"]

    def to_bool(value):
        return True if value == 1 else False

    for col in cols_bool_type:
        df[col] = np.vectorize(to_bool)(df[col])

    def to_string(value):
        return str(int(value)) if not np.isnan(value) else np.nan

    for col, typ in types_cols.items():
        if typ == "string":
            df[col] = np.vectorize(to_string, otypes=[np.ndarray])(df[col])

    return df.rename(columns=RENAMES[table]).rename(columns=str.lower)


# for table in TABLES_TO_PROCESS:
#     table_name = f"{OUTPUT}/{PIRLS_TABLES_DESC[table]}_{table}.parquet"
#     df = clean_data(dfs[table], table)
#     df.to_parquet(table_name, index=False)
#     print(f"Done for {table}")


def parse_scheme(table: str, variable: str) -> list[tuple[str, str]]:
    # O valor na celula ta em outro formato
    if variable == "IDPOP":
        return [("1", "1")]

    df = codebooks[table]
    value = df[df["Variable"] == variable]["Value Scheme Detailed"].values[0].strip()

    def sanitize(value: str) -> tuple[str, str]:
        head, *tail = value.split(":")
        head = head.strip()
        rest = "".join(tail).strip()
        return head, rest

    if not ";" in value:
        return [sanitize(value)]

    return [sanitize(i) for i in value.split(";") if ":" in i]


bd_country_codes = bd.read_sql(
    "SELECT id_pais_m49, nome_oficial_ingles FROM `basedosdados-dev.br_bd_diretorios_mundo.pais`",
    billing_project_id="basedosdados-dev",
)


def build_dict(table: str) -> pd.DataFrame:
    table_arch = pd.DataFrame.copy(
        tables_archs[table][["name", "covered_by_dictionary", "original_name"]]
    )
    cols_covered_by_dict = table_arch[table_arch["covered_by_dictionary"] == "yes"]

    cols_covered_by_dict["id_tabela"] = PIRLS_TABLES_DESC[table]
    cols_covered_by_dict["cobertura_temporal"] = 2021
    cols_covered_by_dict["schema_values"] = cols_covered_by_dict["original_name"].apply(
        lambda variable: parse_scheme(table, variable)
    )
    cols_covered_by_dict = cols_covered_by_dict.explode("schema_values")
    cols_covered_by_dict["chave"] = cols_covered_by_dict["schema_values"].apply(
        lambda schema: schema[0]
    )
    cols_covered_by_dict["valor"] = cols_covered_by_dict["schema_values"].apply(
        lambda schema: schema[1]
    )

    return cols_covered_by_dict.drop(
        ["covered_by_dictionary", "schema_values", "original_name"], axis=1
    ).rename(columns={"name": "nome_coluna"})[
        ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
    ]


pd.concat([build_dict(i) for i in PIRLS_TABLES_DESC.keys()]).to_parquet(
    f"{OUTPUT}/dicionario.parquet", index=False
)
