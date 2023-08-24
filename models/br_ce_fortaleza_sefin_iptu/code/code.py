from shapely import wkt
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd
import numpy as np

df = pd.read_csv('/content/geopandas/Faces de quadra.csv', sep=',')
df = df.rename(columns={
    "quadra_face_id" : "id_face_quadra",
    "the_geom" : "geometria",
    "x" : "latitude",
    "y" : "longitude",
    "agua" : "indicador_agua",
    "esgoto" : "indicador_esgoto",
    "galeria_pluvial" : "indicador_galeria_pluvial",
    "sarjeta" : "indicador_sarjeta",
    "iluminacao_publica" : "indicador_iluminacao_publica",
    "arborizacao" : "indicador_arborizacao"
})


lista = ["indicador_agua",
        "indicador_esgoto",
        "indicador_galeria_pluvial",
        "indicador_sarjeta",
        "indicador_iluminacao_publica",
        "indicador_arborizacao"]

for variaveis_bool in lista:
    df[variaveis_bool] = df[variaveis_bool].apply(lambda x: str(x).replace("Com", "True").replace("Sem", "False"))

df['pavimentacao'] = df['pavimentacao'].str.replace('Paralelep.', 'Paralelepípedo').replace("Sem", "Sem Pavimentação")

df.replace('nan', '', inplace=True)
df['logradouro'] = df['logradouro'].apply(lambda x: ' '.join(str(x).split()))
geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry)
crs = {'init': 'epsg:4326'} # sistema de coordenadas
# Convertendo Point
df['longitude'] = df['longitude'].replace(',', '.')
df['latitude'] = df['latitude'].replace(',', '.')

#Change to Float64
df['longitude'] = df['longitude'].astype(np.float64)
df['latitude'] = df['latitude'].astype(np.float64)

#Assigning the geometry variables
from shapely.geometry.polygon import Point
geometry = [Point(xy) for xy
            in zip(df['longitude'], df['latitude'])]

#For being a point, it became point, if polygon, assign polygon and so on
geo_df = gpd.GeoDataFrame(df,
                          crs = crs,
                          geometry = geometry)

#para projeções geográficas
gdf = gdf.set_crs(epsg=32724)
gdf = gdf.to_crs(epsg=4326)

gdf = gdf.drop(['latitude', 'longitude', 'geometria'], axis=1)
gdf = gdf.rename(columns={'geometry' : 'geometria'})
gdf = gdf[[
    'ano',
    'id_face_quadra',
    'logradouro',
    'metrica',
    'pavimentacao',
    'indicador_agua',
    'indicador_esgoto',
    'indicador_galeria_pluvial',
    'indicador_sarjeta',
    'indicador_iluminacao_publica',
    'indicador_arborizacao',
    'geometria',
    'valor'
]]