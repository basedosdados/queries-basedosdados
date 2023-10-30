import os
import pandas as pd


def gerar_individual_table(file_name: str):
    df = pd.read_csv(file_name, encoding="ISO-8859-1")
    file_name = file_name.split("/")[-1]
    file_name = file_name[:-4]
    bioma, year = file_name.split("_")
    df["bioma"] = bioma
    df["ano"] = int(year)
    df["desmatamento"] = (df[df.columns[7:-5]].sum(axis=1)).round(1)
    df = df[df.columns[[4, 6]].to_list() + df.columns[-6:].to_list()]

    columns_order = ['ano', 'CodIbge', 'bioma', 'AreaKm2', 'desmatamento'] + df.columns[2:5].to_list()
    df = df[columns_order]

    columns_name = ['ano', 'id_municipio', 'bioma', 'area', 'desmatamento', 'floresta', 'nao_floresta', 'hidrografia']
    df.columns = columns_name

    df["floresta"] = (df['area'].values - df[['desmatamento', 'nao_floresta', 'hidrografia']].sum(axis=1)).round(1)

    return df


def get_clean_data() -> None:

    files = os.listdir(os.getcwd().replace("code", "input"))
    files = [f"../input/{file}" for file in files]
    df = pd.concat(map(gerar_individual_table, files))
    df.to_csv("../output/br_inpe_prodes_desmatamento.csv", index=False)


if __name__ == "__main__":
    get_clean_data()
