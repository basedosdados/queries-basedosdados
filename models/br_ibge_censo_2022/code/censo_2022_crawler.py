import pandas as pd
import requests
import os
import basedosdados as bd
import re
from constants import constants
import logging
from tqdm import tqdm

def municipalities_as_chunks(chunk_size: int = 50):
    query = """
    SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """
    df_municipios = bd.read_sql(query, billing_project_id="casebd")
    input_list = list(df_municipios.id_municipio.unique())


    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def sidra_to_dataframe(url: str) -> pd.DataFrame:
    try:
        response = requests.get(url=url)
        if response.status_code >= 400 and response.status_code <= 599:
            logging.info(f"Tabela grande demais: {url}")
            raise Exception(f"Erro de requisição: status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    return pd.json_normalize(response.json())

def rename_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.loc[0, :].values.flatten().tolist()
    return df.iloc[1: , :]

def dataframe_to_parquet(df: pd.DataFrame, mkdir: bool, table_id: str) -> None:
    if mkdir:
        os.makedirs("/tmp/data/br_ibge_censo_2022/input", exist_ok=True)
    return df.to_parquet(path=f"/tmp/data/br_ibge_censo_2022/input/{table_id}.parquet", compression="gzip")


if __name__ == "__main__":
    for k, v in constants.URLS.value.items():
        logging.info(f"Baixando dados da tabela: {k}")
        df_final = pd.DataFrame()
        try:
            df = sidra_to_dataframe(v)
            df = rename_dataframe(df)
            dataframe_to_parquet(df, mkdir = True, table_id=k )
        except:
            output_list = municipalities_as_chunks()
            logging.info(f"Baixando dados em chunks da tabela: {k}")
            for n in tqdm(range(len(output_list))):
                munis = ""
                munis += "".join(f"{value}" if i == 0 else f",{value}" for i, value in enumerate(output_list[n]))
                url_nova = re.split(r"all(?=/v/)", v)
                df = sidra_to_dataframe(url=f"{url_nova[0]}{munis}{url_nova[1]}")
                df = rename_dataframe(df)
                df_final = pd.concat([df_final, df])
            dataframe_to_parquet(df_final, mkdir = True, table_id=k )
