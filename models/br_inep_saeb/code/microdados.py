import polars as pl
import os
import basedosdados as bd
import zipfile

ROOT = os.path.join("models", "br_inep_saeb")
INPUT = os.path.join(ROOT, "input")
TMP = os.path.join(ROOT, "tmp")

ZIP_URL = "https://download.inep.gov.br/microdados/microdados_saeb_2021_ensino_fundamental_e_medio.zip"

os.system(
    f"wget {ZIP_URL} --no-check-certificate -o {INPUT}/{os.path.basename(ZIP_URL)}"
)

ZIP_FILE = os.path.join(INPUT, os.path.basename(ZIP_URL))

with zipfile.ZipFile(ZIP_FILE) as z:
    print(z.namelist())

UNZIP_FILES = [
    "DADOS/TS_ALUNO_2EF.csv",
    "DADOS/TS_ALUNO_5EF.csv",
    "DADOS/TS_ALUNO_9EF.csv",
]

with zipfile.ZipFile(ZIP_FILE) as z:
    for file in UNZIP_FILES:
        z.extract(file, TMP)



df_aluno_2ef = pl.read_csv(os.path.join(TMP, UNZIP_FILES[0]))
