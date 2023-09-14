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

    # Drop last 6 lines
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
    df["chave"] = df["chave"].apply(lambda value: value.split("\n") if isinstance(value, str) and "\n" in value else value)

    assert isinstance(df, pd.DataFrame)
    return df.explode("chave")

dict_by_table = [
    build_dictionary(year, f"{dir_dicts}/{template_file}{year}.xlsx") for year in YEARS
]

pd.concat(dict_by_table).to_csv(f"{OUTPUT}/dicionario_questionarios.csv", index = False)
