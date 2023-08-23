SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(sequencial_candidato AS STRING) sequencial_candidato,
SAFE_CAST(id_candidato_bd AS STRING) id_candidato_bd,
SAFE_CAST(sequencial_certidao AS STRING) sequencial_certidao,
SAFE_CAST(extensao_arquivo AS STRING) extensao_arquivo,
SAFE_CAST(numero_paginas AS INT64) numero_paginas,
SAFE_CAST(texto AS STRING) texto
FROM basedosdados-dev.br_tse_eleicoes_staging.certidoes_criminais AS t