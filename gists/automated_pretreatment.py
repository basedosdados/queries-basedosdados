import pandas as pd


def get_basic_treated_query(dataframe: pd.DataFrame):
    """
    generates a basic treated query
    """

    originais = dataframe["Nome original da coluna"].tolist()
    nomes = dataframe["Nome da coluna"].tolist()
    tipos = dataframe["Tipo da coluna"].tolist()
    for original, nome, tipo in zip(originais, nomes, tipos):
        if tipo == "GEOGRAPHY":
            print(f"ST_GEOGFROMTEXT({original}) AS {nome},")
        elif "id" in nome:
            print(
                f"SAFE_CAST(REGEXP_REPLACE({original}, r'\.0$', '') AS {tipo}) {nome},"
            )
        elif tipo == "DATETIME":
            print(
                f"SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {original}) AS {tipo}) AS {nome},"
            )

        else:
            print(f"SAFE_CAST({original} AS {tipo}) {nome},")
