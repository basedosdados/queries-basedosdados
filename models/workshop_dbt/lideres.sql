WITH candidatos AS (
SELECT data, id_pesquisa, nome_candidato, sigla_partido, percentual, MAX(percentual) OVER (PARTITION BY id_pesquisa, data) AS lider
FROM `basedosdados-dev.br_poder360_pesquisas.microdados` 
WHERE ano=2022
AND cargo="presidente"
AND turno=1
ORDER BY data DESC
)
SELECT id_pesquisa, data, nome_candidato, sigla_partido, percentual, (lider-percentual) AS distancia_lider
FROM candidatos