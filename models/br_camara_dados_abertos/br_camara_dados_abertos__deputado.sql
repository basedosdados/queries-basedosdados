{{ config(alias="deputado", schema="br_camara_dados_abertos") }}
with
    sql as (
        select
            safe_cast(nome as string) nome,
            safe_cast(nome_civil as string) nome_civil,
            safe_cast(data_nascimento as date) data_nascimento,
            safe_cast(data_falecimento as date) data_falecimento,
            regexp_extract(id_deputado, r'/([^/]+)$') as id_deputado,
            case
                when id_municipio_nascimento = 'SAO PAULO'
                then 'São Paulo'
                when id_municipio_nascimento = 'Moji-Mirim'
                then 'Mogi Mirim'
                when id_municipio_nascimento = "São Lourenço D'Oeste"
                then 'São Lourenço do Oeste'
                when id_municipio_nascimento = "Santa Bárbara D'Oeste"
                then "Santa Bárbara d'Oeste"
                when id_municipio_nascimento = "Araióses"
                then "Araioses"
                when id_municipio_nascimento = "Cacador"
                then "Caçador"
                when id_municipio_nascimento = "Pindaré Mirim"
                then "Pindaré-Mirim"
                when id_municipio_nascimento = "Belém de São Francisco"
                then "Belém do São Francisco"
                when id_municipio_nascimento = "Sud Menucci"
                then "Sud Mennucci"
                when id_municipio_nascimento = 'Duerê'
                then "Dueré"
                when id_municipio_nascimento = 'Santana do Livramento'
                then "Sant'Ana do Livramento"
                when id_municipio_nascimento = "Herval D'Oeste"
                then "Herval d'Oeste"
                when id_municipio_nascimento = "Guaçui"
                then "Guaçuí"
                when id_municipio_nascimento = "Lençois Paulista"
                then "Lençóis Paulista"
                when id_municipio_nascimento = "Amambaí"
                then "Amambai"
                when id_municipio_nascimento = "Santo Estevão"
                then "Santo Estêvão"
                when id_municipio_nascimento = "Poxoréu"
                then "Poxoréo"
                when id_municipio_nascimento = "Trajano de Morais"
                then "Trajano de Moraes"
                else id_municipio_nascimento
            end as id_municipio_nascimento,
            safe_cast(sigla_uf_nascimento as string) sigla_uf_nascimento,
            case
                when sexo = 'M'
                then 'Masculino'
                when sexo = 'F'
                then 'Feminino'
                else sexo
            end as sexo,
            safe_cast(id_inicial_legislatura as string) id_inicial_legislatura,
            safe_cast(id_final_legislatura as string) id_final_legislatura,
            safe_cast(url_site as string) url_site,
            safe_cast(url_rede_social as string) url_rede_social,
        from basedosdados - staging.br_camara_dados_abertos_staging.deputado
    ),
    uniao_valores as (
        select a.*, b.nome as name_id_municipio, b.id_municipio, b.sigla_uf
        from sql as a
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as b
            on a.id_municipio_nascimento = b.nome
            and a.sigla_uf_nascimento = b.sigla_uf
    )
select
    nome,
    nome_civil,
    data_nascimento,
    data_falecimento,
    id_municipio as id_municipio_nascimento,
    sigla_uf_nascimento,
    id_deputado,
    sexo,
    id_inicial_legislatura,
    id_final_legislatura,
    url_site,
    url_rede_social,
from uniao_valores
