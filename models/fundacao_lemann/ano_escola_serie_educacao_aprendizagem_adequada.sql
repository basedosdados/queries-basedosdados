WITH saeb_pivot AS (
SELECT
  *
FROM (
  SELECT
    ano,
    id_escola,
    id_aluno,
    serie,
    disciplina,
    CASE
      WHEN ano in (2007, 2009) THEN 1
      ELSE peso_aluno
    END as peso_aluno,
    proficiencia_saeb,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb < 150) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb < 200) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb < 175) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb < 225) THEN 1
      ELSE 0
    END AS insuficiente,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb >= 150 AND proficiencia_saeb < 200) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb >= 200 AND proficiencia_saeb < 275) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb >= 175 AND proficiencia_saeb < 225) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb >= 225 AND proficiencia_saeb < 300) THEN 1
      ELSE 0
    END AS basico,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb >= 200 AND proficiencia_saeb < 250) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb >= 275 AND proficiencia_saeb < 325) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb >= 225 AND proficiencia_saeb < 275) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb >= 300 AND proficiencia_saeb < 350) THEN 1
      ELSE 0
    END AS proficiente,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb >= 250) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb >= 325) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb >= 275) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb >= 350) THEN 1
      ELSE 0
    END AS avancado,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb >= 200) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb >= 275) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb >= 225) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb >= 300) THEN 1
      ELSE 0
    END AS adequado,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb - 16 < 150) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb - 16 < 200) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb - 20 < 175) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb - 20 < 225) THEN 1
      ELSE 0
    END AS insuficiente_pandemia_PB,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 150 AND proficiencia_saeb < 200) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 200 AND proficiencia_saeb < 275) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 175 AND proficiencia_saeb < 225) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 225 AND proficiencia_saeb < 300) THEN 1
      ELSE 0
    END AS basico_pandemia_PB,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 200 AND proficiencia_saeb < 250) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 275 AND proficiencia_saeb < 325) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 225 AND proficiencia_saeb < 275) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 300 AND proficiencia_saeb < 350) THEN 1
      ELSE 0
    END AS proficiente_pandemia_PB,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 250) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 325) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 275) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 350) THEN 1
      ELSE 0
    END AS avancado_pandemia_PB,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 200) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb - 16 >= 275) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 225) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb - 20 >= 300) THEN 1
      ELSE 0
    END AS adequado_pandemia_PB,
    CASE
      WHEN (serie = 5 AND disciplina = 'LP' AND proficiencia_saeb - 12 >= 200) THEN 1
      WHEN (serie = 9 AND disciplina = 'LP' AND proficiencia_saeb - 12 >= 275) THEN 1
      WHEN (serie = 5 AND disciplina = 'MT' AND proficiencia_saeb - 14 >= 225) THEN 1
      WHEN (serie = 9 AND disciplina = 'MT' AND proficiencia_saeb - 14 >= 300) THEN 1
      ELSE 0
    END AS adequado_pandemia_SP
  FROM `basedosdados-dev.br_inep_saeb.proficiencia`
)
PIVOT (
  MAX(proficiencia_saeb)        AS proficiencia_saeb,
  MAX(insuficiente)             AS insuficiente,
  MAX(basico)                   AS basico,
  MAX(proficiente)              AS proficiente,
  MAX(avancado)                 AS avancado,
  MAX(adequado)                 AS adequado,
  MAX(insuficiente_pandemia_PB) AS insuficiente_pandemia_PB,
  MAX(basico_pandemia_PB)       AS basico_pandemia_PB,
  MAX(proficiente_pandemia_PB)  AS proficiente_pandemia_PB,
  MAX(avancado_pandemia_PB)     AS avancado_pandemia_PB,
  MAX(adequado_pandemia_PB)     AS adequado_pandemia_PB,
  MAX(adequado_pandemia_SP)     AS adequado_pandemia_SP
  FOR disciplina in (
    'LP', 'MT'
  )
)
)

SELECT
  *
FROM (
  SELECT
    ano,
    id_escola,
    serie,
    
    SUM(proficiencia_saeb_LP * peso_aluno) / SUM(peso_aluno) as proficiencia_LP,
    SUM(proficiencia_saeb_MT * peso_aluno) / SUM(peso_aluno) as proficiencia_MT,
    
    100 * SUM(insuficiente_LP * peso_aluno) / SUM(peso_aluno) as insuficiente_LP,
    100 * SUM(insuficiente_MT * peso_aluno) / SUM(peso_aluno) as insuficiente_MT,
    100 * SUM(basico_LP       * peso_aluno) / SUM(peso_aluno) as basico_LP,
    100 * SUM(basico_MT       * peso_aluno) / SUM(peso_aluno) as basico_MT,
    100 * SUM(proficiente_LP  * peso_aluno) / SUM(peso_aluno) as proficiente_LP,
    100 * SUM(proficiente_MT  * peso_aluno) / SUM(peso_aluno) as proficiente_MT,
    100 * SUM(avancado_LP     * peso_aluno) / SUM(peso_aluno) as avancado_LP,
    100 * SUM(avancado_MT     * peso_aluno) / SUM(peso_aluno) as avancado_MT,
    100 * SUM(adequado_LP     * peso_aluno) / SUM(peso_aluno) as adequado_LP,
    100 * SUM(adequado_MT     * peso_aluno) / SUM(peso_aluno) as adequado_MT,
    
    100 * SUM(insuficiente_pandemia_PB_LP * peso_aluno) / SUM(peso_aluno) as insuficiente_pandemia_PB_LP,
    100 * SUM(insuficiente_pandemia_PB_MT * peso_aluno) / SUM(peso_aluno) as insuficiente_pandemia_PB_MT,
    100 * SUM(basico_pandemia_PB_LP       * peso_aluno) / SUM(peso_aluno) as basico_pandemia_PB_LP,
    100 * SUM(basico_pandemia_PB_MT       * peso_aluno) / SUM(peso_aluno) as basico_pandemia_PB_MT,
    100 * SUM(proficiente_pandemia_PB_LP  * peso_aluno) / SUM(peso_aluno) as proficiente_pandemia_PB_LP,
    100 * SUM(proficiente_pandemia_PB_MT  * peso_aluno) / SUM(peso_aluno) as proficiente_pandemia_PB_MT,
    100 * SUM(avancado_pandemia_PB_LP     * peso_aluno) / SUM(peso_aluno) as avancado_pandemia_PB_LP,
    100 * SUM(avancado_pandemia_PB_MT     * peso_aluno) / SUM(peso_aluno) as avancado_pandemia_PB_MT,
    100 * SUM(adequado_pandemia_PB_LP     * peso_aluno) / SUM(peso_aluno) as adequado_pandemia_PB_LP,
    100 * SUM(adequado_pandemia_PB_MT     * peso_aluno) / SUM(peso_aluno) as adequado_pandemia_PB_MT,
    100 * SUM(adequado_pandemia_SP_LP     * peso_aluno) / SUM(peso_aluno) as adequado_pandemia_SP_LP,
    100 * SUM(adequado_pandemia_SP_MT     * peso_aluno) / SUM(peso_aluno) as adequado_pandemia_SP_MT,
    
  FROM saeb_pivot
  GROUP BY ano, id_escola, serie
  ORDER BY ano, id_escola, serie ASC
)
ORDER BY id_escola, serie, ano