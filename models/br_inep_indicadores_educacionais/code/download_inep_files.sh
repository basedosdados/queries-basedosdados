#!/bin/bash

urls=(
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/AFD_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/ICG_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/IED_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/ATU_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/HAD_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/DSU_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/IRD_2022_MUNICIPIOS.zip"
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/TDI_2022_MUNICIPIOS.zip"
)

for url in "${urls[@]}"; do
    curl -O "$url"
done