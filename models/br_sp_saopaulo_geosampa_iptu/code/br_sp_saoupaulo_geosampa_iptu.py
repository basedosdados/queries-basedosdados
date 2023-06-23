import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.max_row', None)

anos_tratamento = ['1995',
        '1996',
        '1997',
        '1998',
        '1999',
        '2000',
        '2001',
        '2002',
        '2003',
        '2004',
        '2005',
        '2006',
        '2007',
        '2008',
        '2009',
        '2010',
        '2011',
        '2012',
        '2013',
        '2014',
        '2015',
        '2016',
        '2017',
        '2018',
        '2019',
        '2020',
        '2021',
        '2022',
        '2023']

def to_partitions(data: pd.DataFrame, partition_columns: list[str], savepath: str):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "data.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")

for anos in anos_tratamento:
    print(f'Abrindo o arquivo de {anos}')
    df = pd.read_csv(f'D:\download\iptu\EG_{anos}.csv', sep=';', encoding='utf-8')

    rename = {'NUMERO DO CONTRIBUINTE':'numero_contribuinte',
    'ANO DO EXERCICIO':'ano',
    'NUMERO DA NL':'numero_notificacao',
    'DATA DO CADASTRAMENTO':'data_cadastramento',
    'NUMERO DO CONDOMINIO':'numero_condominio',
    'CODLOG DO IMOVEL':'codigo_logradouro',
    'NOME DE LOGRADOURO DO IMOVEL':'logradouro',
    'NUMERO DO IMOVEL':'numero_imovel',
    'COMPLEMENTO DO IMOVEL':'complemento',
    'BAIRRO DO IMOVEL':'bairro',
    'REFERENCIA DO IMOVEL':'referencia_imovel',
    'CEP DO IMOVEL':'cep',
    'TIPO DE USO DO IMOVEL':'finalidade_imovel',
    'TIPO DE PADRAO DA CONSTRUCAO':'tipo_construcao',
    'TIPO DE TERRENO':'tipo_terreno',
    'QUANTIDADE DE ESQUINAS FRENTES':'quantidade_esquina_imovel',
    'FRACAO IDEAL':'fracao_ideal',
    'AREA DO TERRENO':'area_terreno',
    'AREA CONSTRUIDA':'area_construida',
    'AREA OCUPADA':'area_ocupada',
    'VALOR DO M2 DO TERRENO':'valor_terreno',
    'VALOR DO M2 DE CONSTRUCAO':'valor_construcao',
    'ANO DA CONSTRUCAO CORRIGIDO':'ano_construcao_corrigida',
    'QUANTIDADE DE PAVIMENTOS':'quantidade_pavimento',
    'TESTADA PARA CALCULO':'testada_imovel',
    'ANO DE INICIO DA VIDA DO CONTRIBUINTE':'ano_inicio_vida_contribuinte',
    'MES DE INICIO DA VIDA DO CONTRIBUINTE':'mes_inicio_vida_contribuinte',
    'FASE DO CONTRIBUINTE':'fase_contribuinte',
    'FATOR DE OBSOLESCENCIA':'fator_obsolescencia'}

    ordem = ['ano',
    'data_cadastramento',
    'numero_contribuinte',
    'numero_notificacao',
    'numero_condominio',
    'codigo_logradouro',
    'logradouro',
    'numero_imovel',
    'cep',
    'complemento',
    'bairro',
    'referencia_imovel',
    'finalidade_imovel',
    'tipo_construcao',
    'tipo_terreno',
    'fracao_ideal',
    'area_terreno',
    'area_construida',
    'area_ocupada',
    'testada_imovel',
    'quantidade_esquina_imovel',
    'valor_terreno',
    'valor_construcao',
    'ano_construcao_corrigida',
    'quantidade_pavimento',
    'ano_inicio_vida_contribuinte',
    'mes_inicio_vida_contribuinte',
    'fase_contribuinte',
    'fator_obsolescencia']


    print('renomenado...')
    df.rename(columns=rename, inplace=True)
    print('tratando...')
    lista = ['fator_obsolescencia',
            'testada_imovel',
            'valor_construcao',
            'valor_terreno',
            'fracao_ideal',
            ]
    for x in lista:
        df[x] = df[x].apply(lambda x: str(x).replace(',', '.'))

    cep_codigo_logradouro = ['cep',
                            'codigo_logradouro',
                            'numero_contribuinte']
    for traço in cep_codigo_logradouro:

        df[traço] = df[traço].apply(lambda x: str(x).replace('-', ''))

    nova_lista = ['ano_inicio_vida_contribuinte',
                'mes_inicio_vida_contribuinte',
                'numero_imovel',
                'quantidade_esquina_imovel',
                'area_terreno',
                'area_construida',
                'area_ocupada',
                'ano_construcao_corrigida',
                'quantidade_pavimento',
                'valor_terreno',
                'cep',
                'valor_construcao'
                ]
    for y in nova_lista:
        df[y] = df[y].apply(lambda x: str(x).replace('.0', ''))

    print('formatando data')
    if (df['ano'] <= 2015).all():
        df['data_cadastramento'] = np.nan
    else:
        def formatar_data(data):
            dia = data[:2]
            mes = data[3:5]
            ano = data[6:]
            return f"20{ano}-{mes}-{dia}"

        df['data_cadastramento'] = df['data_cadastramento'].apply(formatar_data)

    df = df.apply(lambda x: x.replace(np.nan, '').replace(',', '.'))

    print('ordenando...')

    df = df[ordem]

    print('dropando duplicados...')

    df = df.drop_duplicates()

    print('particionando...')

    df = df.drop(columns=['fase_contribuinte'])

    to_partitions(
    df,
    partition_columns=['ano'],
    savepath='D:\download\iptu\output'
    )