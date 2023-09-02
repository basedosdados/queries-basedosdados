SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(tipo_organizacao_academica AS STRING) tipo_organizacao_academica,
SAFE_CAST(tipo_categoria_administrativa AS STRING) tipo_categoria_administrativa,
SAFE_CAST(nome_mantenedora AS STRING) nome_mantenedora,
SAFE_CAST(id_mantenedora AS STRING) id_mantenedora,
SAFE_CAST(id_ies AS STRING) id_ies,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(sigla AS STRING) sigla,
SAFE_CAST(endereco AS STRING) endereco,
SAFE_CAST(numero AS STRING) numero,
SAFE_CAST(complemento AS STRING) complemento,
SAFE_CAST(bairro AS STRING) bairro,
SAFE_CAST(cep AS STRING) cep,
SAFE_CAST(quantidade_tecnicos AS INT64) quantidade_tecnicos,
SAFE_CAST(quantidade_tecnicos_ef_incompleto_feminino AS INT64) quantidade_tecnicos_ef_incompleto_feminino,
SAFE_CAST(quantidade_tecnicos_ef_incompleto_masculino AS INT64) quantidade_tecnicos_ef_incompleto_masculino,
SAFE_CAST(quantidade_tecnicos_ef_completo_feminino AS INT64) quantidade_tecnicos_ef_completo_feminino,
SAFE_CAST(quantidade_tecnicos_ef_completo_masculino AS INT64) quantidade_tecnicos_ef_completo_masculino,
SAFE_CAST(quantidade_tecnicos_em_feminino AS INT64) quantidade_tecnicos_em_feminino,
SAFE_CAST(quantidade_tecnicos_em_masculino AS INT64) quantidade_tecnicos_em_masculino,
SAFE_CAST(quantidade_tecnicos_es_feminino AS INT64) quantidade_tecnicos_es_feminino,
SAFE_CAST(quantidade_tecnicos_es_masculino AS INT64) quantidade_tecnicos_es_masculino,
SAFE_CAST(quantidade_tecnicos_especializacao_feminino AS INT64) quantidade_tecnicos_especializacao_feminino,
SAFE_CAST(quantidade_tecnicos_especializacao_masculino AS INT64) quantidade_tecnicos_especializacao_masculino,
SAFE_CAST(quantidade_tecnicos_mestrado_feminino AS INT64) quantidade_tecnicos_mestrado_feminino,
SAFE_CAST(quantidade_tecnicos_mestrado_masculino AS INT64) quantidade_tecnicos_mestrado_masculino,
SAFE_CAST(quantidade_tecnicos_doutorado_feminino AS INT64) quantidade_tecnicos_doutorado_feminino,
SAFE_CAST(quantidade_tecnicos_doutorado_masculino AS INT64) quantidade_tecnicos_doutorado_masculino,
SAFE_CAST(indicador_biblioteca_acesso_portal_capes AS BOOLEAN) indicador_biblioteca_acesso_portal_capes,
SAFE_CAST(indicador_biblioteca_acesso_outras_bases AS BOOLEAN) indicador_biblioteca_acesso_outras_bases,
SAFE_CAST(indicador_biblioteca_assina_outras_bases AS BOOLEAN) indicador_biblioteca_assina_outras_bases,
SAFE_CAST(indicador_biblioteca_repositorio_institucional AS BOOLEAN) indicador_biblioteca_repositorio_institucional,
SAFE_CAST(indicador_biblioteca_busca_integrada AS BOOLEAN) indicador_biblioteca_busca_integrada,
SAFE_CAST(indicador_biblioteca_internet AS BOOLEAN) indicador_biblioteca_internet,
SAFE_CAST(indicador_biblioteca_rede_social AS BOOLEAN) indicador_biblioteca_rede_social,
SAFE_CAST(indicador_biblioteca_catalogo_online AS BOOLEAN) indicador_biblioteca_catalogo_online,
SAFE_CAST(quantidade_biblioteca_periodicos_eletronicos AS INT64) quantidade_biblioteca_periodicos_eletronicos,
SAFE_CAST(quantidade_biblioteca_livros_eletronicos AS INT64) quantidade_biblioteca_livros_eletronicos,
SAFE_CAST(quantidade_docentes AS INT64) quantidade_docentes,
SAFE_CAST(quantidade_docentes_exercicio AS INT64) quantidade_docentes_exercicio,
SAFE_CAST(quantidade_docentes_exercicio_feminino AS INT64) quantidade_docentes_exercicio_feminino,
SAFE_CAST(quantidade_docentes_exercicio_masculino AS INT64) quantidade_docentes_exercicio_masculino,
SAFE_CAST(quantidade_docentes_exercicio_sem_graduacao AS INT64) quantidade_docentes_exercicio_sem_graduacao,
SAFE_CAST(quantidade_docentes_exercicio_graduacao AS INT64) quantidade_docentes_exercicio_graduacao,
SAFE_CAST(quantidade_docentes_exercicio_especializacao AS INT64) quantidade_docentes_exercicio_especializacao,
SAFE_CAST(quantidade_docentes_exercicio_mestrado AS INT64) quantidade_docentes_exercicio_mestrado,
SAFE_CAST(quantidade_docentes_exercicio_doutorado AS INT64) quantidade_docentes_exercicio_doutorado,
SAFE_CAST(quantidade_docentes_exercicio_integral AS INT64) quantidade_docentes_exercicio_integral,
SAFE_CAST(quantidade_docentes_exercicio_integral_dedicacao_exclusiva AS INT64) quantidade_docentes_exercicio_integral_dedicacao_exclusiva,
SAFE_CAST(quantidade_docentes_exercicio_integral_sem_dedicacao_exclusiva AS INT64) quantidade_docentes_exercicio_integral_sem_dedicacao_exclusiva,
SAFE_CAST(quantidade_docentes_exercicio_parcial AS INT64) quantidade_docentes_exercicio_parcial,
SAFE_CAST(quantidade_docentes_exercicio_horista AS INT64) quantidade_docentes_exercicio_horista,
SAFE_CAST(quantidade_docentes_exercicio_0_29 AS INT64) quantidade_docentes_exercicio_0_29,
SAFE_CAST(quantidade_docentes_exercicio_30_34 AS INT64) quantidade_docentes_exercicio_30_34,
SAFE_CAST(quantidade_docentes_exercicio_35_39 AS INT64) quantidade_docentes_exercicio_35_39,
SAFE_CAST(quantidade_docentes_exercicio_40_44 AS INT64) quantidade_docentes_exercicio_40_44,
SAFE_CAST(quantidade_docentes_exercicio_45_49 AS INT64) quantidade_docentes_exercicio_45_49,
SAFE_CAST(quantidade_docentes_exercicio_50_54 AS INT64) quantidade_docentes_exercicio_50_54,
SAFE_CAST(quantidade_docentes_exercicio_55_59 AS INT64) quantidade_docentes_exercicio_55_59,
SAFE_CAST(quantidade_docentes_exercicio_60_mais AS INT64) quantidade_docentes_exercicio_60_mais,
SAFE_CAST(quantidade_docentes_exercicio_branca AS INT64) quantidade_docentes_exercicio_branca,
SAFE_CAST(quantidade_docentes_exercicio_preta AS INT64) quantidade_docentes_exercicio_preta,
SAFE_CAST(quantidade_docentes_exercicio_parda AS INT64) quantidade_docentes_exercicio_parda,
SAFE_CAST(quantidade_docentes_exercicio_amarela AS INT64) quantidade_docentes_exercicio_amarela,
SAFE_CAST(quantidade_docentes_exercicio_indigena AS INT64) quantidade_docentes_exercicio_indigena,
SAFE_CAST(quantidade_docentes_exercicio_cor_nao_declarada AS INT64) quantidade_docentes_exercicio_cor_nao_declarada,
SAFE_CAST(quantidade_docentes_exercicio_brasileiro AS INT64) quantidade_docentes_exercicio_brasileiro,
SAFE_CAST(quantidade_docentes_exercicio_estrangeiro AS INT64) quantidade_docentes_exercicio_estrangeiro,
SAFE_CAST(quantidade_docentes_exercicio_deficiencia AS INT64) quantidade_docentes_exercicio_deficiencia
FROM basedosdados-staging.br_inep_censo_educacao_superior_staging.ies AS t