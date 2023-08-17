import pandas as pd
import numpy as np
df = pd.read_csv('/mnt/d/download/br_ce_sefin_iptu/Faces de quadra.csv', sep=',')
df = df.rename(columns={
    "quadra_face_id" : "id_face_quadra",
    "the_geom" : "geometria",
    "x" : "latitude",
    "y" : "longitude",
})
df = df[[
    'ano',
    'id_face_quadra',
    'logradouro',
    'geometria',
    'metrica',
    'pavimentacao',
    'agua',
    'esgoto',
    'galeria_pluvial',
    'sarjeta',
    'iluminacao_publica',
    'arborizacao',
    'latitude',
    'longitude',
    'valor'
]]
lista = ["agua",
        "esgoto",
        "galeria_pluvial",
        "sarjeta",
        "iluminacao_publica",
        "arborizacao"]
for variaveis_bool in lista:
    df[variaveis_bool] = df[variaveis_bool].apply(lambda x: str(x).replace("Com", "True").replace("Sem", "False"))
df['pavimentacao'] = df['pavimentacao'].str.replace('Paralelep.', 'Paralelepípedo').replace("Sem", "Sem Pavimentação")
df.replace('nan', '', inplace=True)
df['logradouro'] = df['logradouro'].apply(lambda x: ' '.join(str(x).split()))
df.to_csv('/mnt/d/download/br_ce_sefin_iptu/microdados.csv', index=False, encoding='utf-8')