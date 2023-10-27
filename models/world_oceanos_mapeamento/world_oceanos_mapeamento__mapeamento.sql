{{ 
  config(
    alias = 'mapeamento',
    schema='world_oceanos_mapeamento',
    materialized='table' )
 }}
SELECT
SAFE_CAST(id AS STRING) id,
SAFE_CAST(livro_titulo AS STRING) titulo,
SAFE_CAST(livro_genero_literario AS STRING) genero_literario,
SAFE_CAST(livro_outros_generos_literarios AS FLOAT64) outros_generos_literarios,
SAFE_CAST(livro_registro_linguistico AS STRING) registro_linguistico,
SAFE_CAST(livro_tematica AS STRING) tematica,
SAFE_CAST(livro_espaco_de_representacao AS STRING) espaco_representacao,
SAFE_CAST(livro_ambiente_predominante AS STRING) ambiente_predominante,
SAFE_CAST(livro_temporalidade AS STRING) temporalidade,
SAFE_CAST(livro_foco_narrativo AS STRING) foco_narrativo,
SAFE_CAST(livro_tipo_de_narrador AS STRING) tipo_narrador,
SAFE_CAST(livro_procedimento_expressivo AS STRING) procedimento_expressivo,
SAFE_CAST(livro_genero_dramaturgico AS STRING) genero_dramaturgico,
SAFE_CAST(livro_interprete AS STRING) interprete,
SAFE_CAST(livro_narrador AS STRING) narrador,
SAFE_CAST(livro_formato_de_cena AS STRING) formato_cena,
SAFE_CAST(livro_estetica_cenografica AS STRING) estetica_cenografica,
SAFE_CAST(livro_tipo_localizacao_pred AS STRING) tipo_localizacao,
SAFE_CAST(livro_localizacao_geografica AS STRING) localizacao_geografica,
FROM basedosdados-staging.world_oceanos_mapeamento_staging.mapeamento AS t

