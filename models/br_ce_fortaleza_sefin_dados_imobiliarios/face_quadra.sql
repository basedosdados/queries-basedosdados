SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(id_face_quadra AS STRING) id_face_quadra,
SAFE_CAST(logradouro AS STRING) logradouro,
SAFE_CAST(metrica AS STRING) metrica,
SAFE_CAST(pavimentacao AS STRING) pavimentacao,
SAFE_CAST(agua AS BOOL) agua,
SAFE_CAST(esgoto AS BOOL) esgoto,
SAFE_CAST(galeria_pluvial AS BOOL) galeria_pluvial,
SAFE_CAST(sarjeta AS BOOL) sarjeta,
SAFE_CAST(iluminacao_publica AS BOOL) iluminacao_publica,
SAFE_CAST(arborizacao AS BOOL) arborizacao,
SAFE_CAST(latitude AS float64) latitude,
SAFE_CAST(longitude AS float64) longitude,
SAFE_CAST(geometria AS STRING) geometria,
SAFE_CAST(valor AS float64) valor
FROM basedosdados-staging.br_ce_fortaleza_sefin_dados_imobiliarios_staging.face_quadra AS t