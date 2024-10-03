import os
import numpy as np
import pandas as pd
from clean_functions import *

def rename_columns(df):
    name_dict = {
        'Ano':'ano',
        'Mês':'mes',
        'Unidade da Federação':'nome_uf',
        'Região Política':'regiao_politica',
        'Cidade e UF':'cidade_uf',
        'Valor':'valor_arrecadado'
    }

    return df.rename(columns=name_dict)

def change_types(df):
    df['ano'] = df['ano'].astype('int')
    df['mes'] = get_month_number(df['mes'])
    df['valor_arrecadado'] = df['valor_arrecadado'].apply(replace_commas).apply(remove_dots).astype('float')

    return df

if __name__ == '__main__':
    df = read_data(file_dir='../input/arrecadacao-itr.csv')
    df = remove_empty_rows(df)
    df = rename_columns(df)
    df = change_types(df)
    save_data(df=df,file_dir='../output/br_rf_arrecadacao_itr',partition_cols=['ano','mes'])
