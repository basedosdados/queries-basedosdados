#!/bin/bash

# Gera um yaml com o schema da tabela

# Para rodar esse script o usuário deve rodar:
# bash schema_as_yml.sh [dataset] [table]
# fazendo as substituições pertinentes

dataset=$1
table=$2

bq show --schema basedosdados-projetos:$dataset.$table  | jq '.' | yq -P | egrep -v type