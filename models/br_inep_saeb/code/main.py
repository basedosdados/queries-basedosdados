import pandas as pd
import os

os.getcwd()

INPUT = os.path.join(os.getcwd(), "input")
URL = "https://download.inep.gov.br/saeb/resultados/saeb_2021_brasil_estados_municipios_c_tx_alfabetizado.xlsx"

os.makedirs(INPUT, exist_ok=True)

os.system(f"cd {INPUT}; curl -O -k {URL}")

df_br = pd.read_excel(os.path.join(INPUT, os.path.basename(URL)), sheet_name="Brasil")

df_br.columns

df_br["ID"].unique()

df_br["ANO_SAEB"].unique()

df_br["DEPENDENCIA_ADM"].unique()

df_br["LOCALIZACAO"].unique()

df_br["CAPITAL"].unique()

df_br["TX_ALFABETIZADO"].unique()

df_br["DEPENDENCIA_ADM"] = df_br["DEPENDENCIA_ADM"].str.lower()

df_br["LOCALIZACAO"] = df_br["LOCALIZACAO"].str.lower()


df_br
