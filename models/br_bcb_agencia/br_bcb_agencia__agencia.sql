{{ 
  config(
    alias='agencia',
    schema='br_bcb_agencia',
    materialized='incremental',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2007,
        "end": 2024,
        "interval": 1}
     },
     pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
     post_hook = [ 
      'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
      'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
       ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)'      
     ]   
    )
 }}


WITH wrang_data as (
SELECT
  CASE
    WHEN sigla_uf = 'SP' AND nome =  'mogimirim' THEN '3530805'
    WHEN sigla_uf = 'SP' AND nome =  'mogiguacu' THEN '3530706'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia ceilandia' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia brazlandia' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia sobradinho' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia samambaia' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia gama' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia taguatinga' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia guara' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia paranoa' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia nucleo bandeirante' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia cruzeiro' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia sudoesteoctogonal' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia aguas claras' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia planaltina' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia recanto das emas' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia santa maria' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia riacho fundo' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia sao sebastiao' THEN '5300108'
    WHEN sigla_uf = 'DF' AND nome = 'brasilia candangolandia' THEN '5300108'
    WHEN sigla_uf = 'RJ' AND nome = 'trajano de morais' THEN '3305901'
    WHEN sigla_uf = 'RS' AND nome = 'entre ijuis' THEN '4306932'
    WHEN sigla_uf = 'MG' AND nome = 'brasopolis' THEN '3108909'
    WHEN sigla_uf = 'PR' AND nome = 'santa cruz do monte castelo' THEN '4123303'
    WHEN sigla_uf = 'PA' AND nome = 'eldorado dos carajas' THEN '1502954'
    WHEN sigla_uf = 'PE' AND nome = 'belem de sao francisco' THEN '2601607'
    WHEN sigla_uf = 'SC' AND nome = 'sao lourenco doeste' THEN '4216909'
    WHEN sigla_uf = 'MG' AND nome = 'sao tome das letras' THEN '3165206'
    WHEN sigla_uf = 'MG' AND nome = 'dona euzebia' THEN '3122900'
    WHEN sigla_uf = 'SC' AND nome = 'picarras' THEN '4212809'
    WHEN sigla_uf = 'SP' AND nome = 'florinea' THEN '3516101'
    WHEN sigla_uf = 'MA' AND nome = 'pindare mirim' THEN '2108504'
    WHEN sigla_uf = 'SC' AND nome = 'presidente castelo branco' THEN '4120408'
    WHEN sigla_uf = 'RO' AND nome = 'alta floresta do oeste' THEN '1100015'
    WHEN sigla_uf = 'PB' AND nome = 'campo de santana' THEN '2516409'
    WHEN sigla_uf = 'RN' AND nome = 'augusto severo' THEN '2401305'
    WHEN sigla_uf = 'SC' AND nome = 'luis alves' THEN '4210001'
    WHEN sigla_uf = 'SP' AND nome = 'luisiania' THEN '3527702'
    WHEN sigla_uf = 'RO' AND nome = 'alvorada do oeste' THEN '1100346'
    WHEN sigla_uf = 'RO' AND nome = 'santa luzia do oeste' THEN '1100296'
    WHEN sigla_uf = 'PE' AND nome = 'itamaraca' THEN '2607604'
    WHEN sigla_uf = 'RS' AND nome = 'chiapeta' THEN '4305405'
    WHEN sigla_uf = 'MG' AND nome = 'itabirinha de mantena' THEN '3131802'
    WHEN sigla_uf = 'MS' AND nome = 'bataipora' THEN '3528502'
    WHEN sigla_uf = 'SP' AND nome = 'brodosqui' THEN '3507803'
    WHEN sigla_uf = 'TO' AND nome = 'paraiso do norte de goias' THEN '1716109'
    WHEN sigla_uf = 'PE' AND nome = 'cabo' THEN '2602902'
    WHEN sigla_uf = 'TO' AND nome = 'miracema do norte' THEN '1713205'
    WHEN sigla_uf = 'RJ' AND nome = 'pati do alferes' THEN '3303856'
    WHEN sigla_uf = 'TO' AND nome = 'colinas de goias' THEN '1705508'
    WHEN sigla_uf = 'RN' AND nome = 'assu' THEN '2400208'
    WHEN sigla_uf = 'BA' AND nome = 'camaca' THEN '2905602'
    WHEN sigla_uf = 'SE' AND nome = 'caninde do sao francisco' THEN '2801207'
    WHEN sigla_uf = 'MT' AND nome = 'quatro marcos' THEN '5107107'
    WHEN sigla_uf = 'SP' AND nome = 'ipaucu' THEN '3520905'
    WHEN sigla_uf = 'MT' AND nome = 'rio claro' THEN '3543907'
    WHEN sigla_uf = 'SP' AND nome = 'sud menucci' THEN '3552304'
    WHEN sigla_uf = 'RS' AND nome = 'eldorado' THEN '4306767'
    WHEN sigla_uf = 'RS' AND nome = 'portolandia' THEN '5218102'
    WHEN sigla_uf = 'MG' AND nome = 'gouvea' THEN '3127602'
    WHEN sigla_uf = 'MG' AND nome = 'sao joao da manteninha' THEN '3162575'
    WHEN sigla_uf = 'MT' AND nome = 'vila bela da sstrindade' THEN '5105507'
    WHEN sigla_uf = 'SP' AND nome = 'salmorao' THEN '3545100'
    WHEN sigla_uf = 'MG' AND nome = 'gouveia' THEN '3127602'
    WHEN sigla_uf = 'MT' AND nome = 'poxoreu' THEN '5107008'
    WHEN sigla_uf = 'GO' AND nome = 'portolandia' THEN '5218102'
    WHEN sigla_uf = 'TO' AND nome = 'alianca do norte' THEN '1700350'
    WHEN sigla_uf = 'MA' AND nome = 'sao luiz gonzaga maranhao' THEN '2111409'
    WHEN sigla_uf = 'MG' AND nome = 'cachoeira do pajeu' THEN '3102704'
    WHEN sigla_uf = 'TO' AND nome = 'divinopolis de goias' THEN '1707108'
    WHEN sigla_uf = 'GO' AND nome = 'cocalzinho' THEN '5205513'
    WHEN sigla_uf = 'RO' AND nome = 'sao francisco do guarope' THEN '1101492'
    WHEN sigla_uf = 'PE' AND nome = 'lagoa do itaenga' THEN '2608503'
    WHEN sigla_uf = 'RJ' AND nome = 'parati' THEN '3303807'
    WHEN sigla_uf = 'SC' AND nome = 'sao miguel doeste' THEN '4217204'
    WHEN sigla_uf = 'PR' AND nome = 'rosario' THEN '4122651'
    WHEN sigla_uf = 'AM' AND nome = 'careiro castanho' THEN '1301100'
    WHEN sigla_uf = 'SP' AND nome = 'embu' THEN '3515004'
    WHEN sigla_uf = 'RO' AND nome = 'nova brasilandia' THEN '1100148'
    WHEN sigla_uf = 'GO' AND nome = 'costelandia' THEN '5205059'
    ELSE id_municipio
  END as id_municipio_fixed,
  CASE
    WHEN LENGTH(cnpj) != 14 
    THEN NULL
    ELSE cnpj
  END AS cnpj1,  
    LPAD(cep, 8, '0') as cep1,
    NULLIF(sigla_uf, 'nan') as sigla_uf1,
    NULLIF(nome_agencia, 'nan') as nome_agencia1,
    NULLIF(instituicao, 'nan') as instituicao1,
    NULLIF(segmento, 'nan') as segmento1,
    NULLIF(id_compe_bcb_agencia, 'nan') as id_compe_bcb_agencia1,
    NULLIF(id_compe_bcb_instituicao, 'nan') as id_compe_bcb_instituicao1,
    NULLIF(endereco, 'nan') as endereco1,
    NULLIF(complemento, 'nan') as complemento1,
    NULLIF(bairro, 'nan') as bairro1,
    NULLIF(ddd, 'nan') as ddd1,
    NULLIF(fone, 'nan') as fone1,
    NULLIF(id_instalacao, 'nan') as id_instalacao1,
    data_inicio,
    ano,
    mes
  FROM basedosdados-staging.br_bcb_agencia_staging.agencia AS t
)

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf1 AS STRING) sigla_uf,
SAFE_CAST(NULLIF(id_municipio_fixed, 'nan') AS STRING) id_municipio,
SAFE_CAST(data_inicio AS DATE) data_inicio,
SAFE_CAST(cnpj1 AS STRING) cnpj,
SAFE_CAST(nome_agencia1 AS STRING) nome_agencia,
SAFE_CAST(instituicao1 AS STRING) instituicao,
SAFE_CAST(segmento1 AS STRING) segmento,
SAFE_CAST(id_compe_bcb_agencia1 AS STRING) id_compe_bcb_agencia,
SAFE_CAST(id_compe_bcb_instituicao1 AS STRING) id_compe_bcb_instituicao,
CASE
  WHEN REGEXP_CONTAINS(cep1, r'^0{8}$') 
  THEN NULL
  else cep1
  end as cep,
SAFE_CAST(endereco1 AS STRING) endereco,
SAFE_CAST(complemento1 AS STRING) complemento,
SAFE_CAST(bairro1 AS STRING) bairro,
SAFE_CAST(ddd1 AS STRING) ddd,
SAFE_CAST(fone1 AS STRING) fone,
SAFE_CAST(id_instalacao1 AS STRING) id_instalacao
FROM wrang_data
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}


