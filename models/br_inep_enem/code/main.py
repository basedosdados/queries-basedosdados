import pandas as pd
import requests
import zipfile
import io
import os

BASE_URL = "https://download.inep.gov.br/microdados"
YEARS = range(1998, 2022 + 1)

# CWD = os.path.dirname(os.getcwd())
CWD = os.getcwd()
INPUT = os.path.join(CWD, "input")
TMP = os.path.join(CWD, "tmp")
OUTPUT = os.path.join(CWD, "output")

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

if not os.path.exists(TMP):
    os.mkdir(TMP)

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)


def make_url(year: int) -> str:
    return f"{BASE_URL}/microdados_enem_{year}.zip"


def download_file(year: int) -> None:
    if os.path.exists(f"{INPUT}/{year}"):
        print(f"Data for {year} already exists")
        return None
    url = make_url(year)
    r = requests.get(url, verify=False)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(f"{INPUT}/{year}")
    return None


for year in YEARS:
    download_file(year)


def extract_dicts() -> tuple[str, str]:
    z = zipfile.ZipFile(f"{INPUT}/dicionarios-20230914T203333Z-001.zip")
    z.extractall(TMP)
    return (f"{TMP}/dicionarios", "Dicionário_Microdados_ENEM_")


dir_dicts, template_file = extract_dicts()


def build_dictionary(year: int, path: str) -> pd.DataFrame:
    # dict_folder_name = [
    #     folder for folder in os.listdir(f"{INPUT}/{year}") if folder.startswith("DICIO")
    # ]

    # folder = dict_folder_name[0]

    df = pd.read_excel(path)

    first_col = df.columns[0]
    assert isinstance(first_col, str) and first_col.startswith("DICIONÁRIO")

    line_separator = (
        f"QUESTIONÁRIO SOCIOECONÔMICO DO ENEM"
        if year < 2010
        else "DADOS DO QUESTIONÁRIO SOCIOECONÔMICO"
    )

    # print(f"{first_col=}, {line_separator=}")

    start_line = df[df[first_col].str.contains(line_separator, na=False)].index[0]

    df = df[df.index > start_line]

    assert isinstance(df, pd.DataFrame)

    columns = {
        "Unnamed: 1": "descricao",
        "Unnamed: 2": "chave",
        "Unnamed: 3": "valor",
        "Unnamed: 4": "tamanho",
        "Unnamed: 5": "tipo",
    }

    columns[first_col] = "coluna"

    df = df.rename(columns=columns, errors="raise")

    # Drop lines here "chave" is empty
    df = df[df["chave"].notna()]

    assert isinstance(df, pd.DataFrame)

    cols = df["coluna"].to_list()

    for index in range(0, len(cols) + 1):
        next_index = index + 1
        if next_index < len(cols) and pd.isna(cols[next_index]):
            cols[next_index] = cols[index]

    df["coluna"] = cols
    df["cobertura_temporal"] = str(year)
    df["id_tabela"] = f"questionario_socioeconomico_{year}"

    df = df[["id_tabela", "coluna", "chave", "cobertura_temporal", "valor"]]
    df = df[df["coluna"] != "IN_QSE"]

    # Some records contains multiple values
    df["chave"] = df["chave"].apply(lambda value: value.split("\n") if isinstance(value, str) and "\n" in value else value)  # type: ignore

    assert isinstance(df, pd.DataFrame)
    df = df.explode("chave")

    cols_with_empty_value = df[df["valor"].isna()]["coluna"].unique()  # type: ignore

    for col in cols_with_empty_value:
        valid_value = df.loc[
            (df["coluna"] == col) & (df["valor"].notna()), "valor"
        ].values
        assert len(valid_value) == 1
        df.loc[df["coluna"] == col, "valor"] = valid_value[0]

    return df


dict_by_table = [
    build_dictionary(year, f"{dir_dicts}/{template_file}{year}.xlsx") for year in YEARS
]

pd.concat(dict_by_table).to_csv(f"{OUTPUT}/dicionario_questionarios.csv", index=False)


def read_remote_sheet(url):
    url = url.replace("edit#gid=", "export?format=csv&gid=")
    return pd.read_csv(
        io.StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )


microdados_arch = read_remote_sheet(
    "https://docs.google.com/spreadsheets/d/1EUhqjdB6BDGlksgy4UY8cwTF7pQBavP7Mrhgi-y3GRI/edit#gid=0"
)
microdados_arch = microdados_arch[microdados_arch["covered_by_dictionary"] == "yes"]


def build_dictionary_microdados(year: int, path: str, cols_filled: list[str]):
    df = pd.read_excel(path)

    first_col = df.columns[0]
    assert isinstance(first_col, str) and first_col.startswith("DICIONÁRIO")

    line_end_separator = (
        f"QUESTIONÁRIO SOCIOECONÔMICO DO ENEM"
        if year < 2010
        else "DADOS DO QUESTIONÁRIO SOCIOECONÔMICO"
    )

    start_line = df[df[first_col].str.contains("NU_INSCRICAO", na=False)].index[0]
    end_line = df[df[first_col].str.contains(line_end_separator, na=False)].index[0]

    df = df[(df.index >= start_line) & (df.index < end_line)]

    columns = {
        "Unnamed: 1": "descricao",
        "Unnamed: 2": "chave",
        "Unnamed: 3": "valor",
        "Unnamed: 4": "tamanho",
        "Unnamed: 5": "tipo",
    }
    columns[first_col] = "variavel"

    df = df.rename(columns=columns, errors="raise")  # type: ignore

    cols_filled = df["variavel"].to_list()

    for index in range(0, len(cols_filled) + 1):
        next_index = index + 1
        if next_index < len(cols_filled) and pd.isna(cols_filled[next_index]):
            cols_filled[next_index] = cols_filled[index]

    df["variavel"] = cols_filled

    assert isinstance(df, pd.DataFrame)
    return df


a = build_dictionary_microdados(
    1998, f"{dir_dicts}/{template_file}{1998}.xlsx", microdados_arch["name"].to_list()
)
a["variavel"].to_list()

a.tail()
a.columns

a[a["variavel"] == "TP_FAIXA_ETARIA"]

microdados_arch["name"].to_list()
