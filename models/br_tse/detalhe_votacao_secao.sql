WITH votacao AS (
  SELECT *
  FROM
 `basedosdados-dev.br_tse_staging.detalhes_votacao_secao`
)
SELECT
SAFE_CAST(ano AS INT64) ano,
sigla_uf,
PARSE_DATE('%d/%m/%Y',  dt_geracao) AS dt_geracao,
PARSE_TIME("%H:%M:%S",hh_geracao) AS hh_geracao,
SAFE_CAST(cd_tipo_eleicao AS INT64) cd_tipo_eleicao,
nm_tipo_eleicao,
SAFE_CAST(nr_turno AS INT64) nr_turno,
SAFE_CAST(cd_eleicao AS INT64) cd_eleicao,
ds_eleicao,
PARSE_DATE('%d/%m/%Y',  dt_eleicao) AS dt_eleicao,
tp_abrangencia,
sg_ue,
nm_ue,
cd_municipio,
nm_municipio,
nr_zona,
nr_secao,
cd_cargo,
nr_votavel,
nm_votavel,
SAFE_CAST(qt_votos AS INT64) qt_votos,
nr_local_votacao
FROM votacao
