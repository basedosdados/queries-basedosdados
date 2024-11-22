with
    sql_chik as (
        select
            safe_cast(trim(ano) as int64) ano,
            safe_cast(trim(tp_not) as string) tipo_notificacao,
            case
                when id_agravo = 'A92.0'
                then 'A92'
                when id_agravo = 'A920'
                then 'A92'
                when id_agravo = 'A92.'
                then 'A92'
                else safe_cast(trim(id_agravo) as string)
            end id_agravo,
            case
                when trim(dt_notific) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_notific, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_notificacao,
            case
                when length(trim(sem_not)) = 5
                then
                    safe_cast(
                        trim(
                            '2' || substring(trim(sem_not), 1, length(trim(sem_not)))
                        ) as int64
                    )
                else safe_cast(trim(sem_not) as int64)
            end semana_notificacao,
            case
                when trim(sigla_uf_notificacao) = '11'
                then 'RO'
                when trim(sigla_uf_notificacao) = '12'
                then 'AC'
                when trim(sigla_uf_notificacao) = '13'
                then 'AM'
                when trim(sigla_uf_notificacao) = '14'
                then 'RR'
                when trim(sigla_uf_notificacao) = '15'
                then 'PA'
                when trim(sigla_uf_notificacao) = '16'
                then 'AP'
                when trim(sigla_uf_notificacao) = '17'
                then 'TO'
                when trim(sigla_uf_notificacao) = '21'
                then 'MA'
                when trim(sigla_uf_notificacao) = '22'
                then 'PI'
                when trim(sigla_uf_notificacao) = '23'
                then 'CE'
                when trim(sigla_uf_notificacao) = '24'
                then 'RN'
                when trim(sigla_uf_notificacao) = '25'
                then 'PB'
                when trim(sigla_uf_notificacao) = '26'
                then 'PE'
                when trim(sigla_uf_notificacao) = '27'
                then 'AL'
                when trim(sigla_uf_notificacao) = '28'
                then 'SE'
                when trim(sigla_uf_notificacao) = '29'
                then 'BA'
                when trim(sigla_uf_notificacao) = '31'
                then 'MG'
                when trim(sigla_uf_notificacao) = '32'
                then 'ES'
                when trim(sigla_uf_notificacao) = '33'
                then 'RJ'
                when trim(sigla_uf_notificacao) = '35'
                then 'SP'
                when trim(sigla_uf_notificacao) = '41'
                then 'PR'
                when trim(sigla_uf_notificacao) = '42'
                then 'SC'
                when trim(sigla_uf_notificacao) = '43'
                then 'RS'
                when trim(sigla_uf_notificacao) = '50'
                then 'MS'
                when trim(sigla_uf_notificacao) = '51'
                then 'MT'
                when trim(sigla_uf_notificacao) = '52'
                then 'GO'
                when trim(sigla_uf_notificacao) = '53'
                then 'DF'
                else safe_cast(trim(sigla_uf_notificacao) as string)
            end sigla_uf_notificacao,
            case
                when trim(id_regiona) = ''
                then null
                when regexp_contains(trim(id_regiona), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(id_regiona), '[a-zA-Z]')
                then null
                else safe_cast(trim(id_regiona) as string)
            end id_regional_saude_notificacao,
            case
                when trim(id_municip) = ''
                then null
                when regexp_contains(trim(id_municip), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(id_municip), '[a-zA-Z]')
                then null
                else safe_cast(trim(id_municip) as string)
            end id_municipio_notificacao,
            case
                when trim(id_unidade) = ''
                then null
                when regexp_contains(trim(id_unidade), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(id_unidade), '[a-zA-Z]')
                then null
                else safe_cast(trim(id_unidade) as string)
            end id_estabelecimento,
            case
                when trim(dt_sin_pri) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_sin_pri, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_primeiros_sintomas,
            case
                when trim(sem_pri) = ''
                then null
                when length(trim(sem_pri)) = 4
                then
                    safe_cast(
                        trim(
                            '20' || substring(trim(sem_pri), 1, length(trim(sem_pri)))
                        ) as int64
                    )
                when length(trim(sem_pri)) = 5 and substring(trim(sem_pri), 1, 1) = '0'
                then
                    safe_cast(
                        trim(
                            '2' || substring(trim(sem_pri), 1, length(trim(sem_pri)))
                        ) as int64
                    )
                when
                    length(trim(sem_pri)) = 6
                    and substring(trim(sem_pri), 1, length(trim(sem_pri))) like '10%%%%'
                then
                    safe_cast(
                        concat(
                            '2', substring(trim(sem_pri), 2, length(trim(sem_pri)))
                        ) as int64
                    )
                when
                    length(trim(sem_pri)) = 5
                    and substring(trim(sem_pri), 1, length(trim(sem_pri))) like '2%%%%'
                then
                    safe_cast(
                        concat(
                            substring(trim(sem_pri), 1, 1),
                            '0',
                            substring(trim(sem_pri), 2)
                        ) as int64
                    )
                else safe_cast(trim(sem_pri) as int64)
            end semana_sintomas,
            case
                when trim(id_pais) = ''
                then null
                when regexp_contains(id_pais, '[^a-zA-Z0-9 ]')
                then null
                else safe_cast(trim(id_pais) as string)
            end pais_residencia,
            case
                when trim(sg_uf) = ''
                then null
                when regexp_contains(sg_uf, '[^a-zA-Z0-9 ]')
                then null
                when trim(sg_uf) = '11'
                then 'RO'
                when trim(sg_uf) = '12'
                then 'AC'
                when trim(sg_uf) = '13'
                then 'AM'
                when trim(sg_uf) = '14'
                then 'RR'
                when trim(sg_uf) = '15'
                then 'PA'
                when trim(sg_uf) = '16'
                then 'AP'
                when trim(sg_uf) = '17'
                then 'TO'
                when trim(sg_uf) = '21'
                then 'MA'
                when trim(sg_uf) = '22'
                then 'PI'
                when trim(sg_uf) = '23'
                then 'CE'
                when trim(sg_uf) = '24'
                then 'RN'
                when trim(sg_uf) = '25'
                then 'PB'
                when trim(sg_uf) = '26'
                then 'PE'
                when trim(sg_uf) = '27'
                then 'AL'
                when trim(sg_uf) = '28'
                then 'SE'
                when trim(sg_uf) = '29'
                then 'BA'
                when trim(sg_uf) = '31'
                then 'MG'
                when trim(sg_uf) = '32'
                then 'ES'
                when trim(sg_uf) = '33'
                then 'RJ'
                when trim(sg_uf) = '35'
                then 'SP'
                when trim(sg_uf) = '41'
                then 'PR'
                when trim(sg_uf) = '42'
                then 'SC'
                when trim(sg_uf) = '43'
                then 'RS'
                when trim(sg_uf) = '50'
                then 'MS'
                when trim(sg_uf) = '51'
                then 'MT'
                when trim(sg_uf) = '52'
                then 'GO'
                when trim(sg_uf) = '53'
                then 'DF'
                else safe_cast(trim(sg_uf) as string)
            end sigla_uf_residencia,
            case
                when trim(id_rg_resi) = ''
                then null
                when regexp_contains(id_rg_resi, '[^a-zA-Z0-9 ]')
                then null
                else safe_cast(trim(id_rg_resi) as string)
            end id_regional_saude_residencia,
            case
                when trim(id_mn_resi) = ''
                then null
                when regexp_contains(id_mn_resi, '[^a-zA-Z0-9 ]')
                then null
                else safe_cast(trim(id_mn_resi) as string)
            end id_municipio_residencia,
            case
                when trim(ano_nasc) = ''
                then null
                else safe_cast(trim(ano_nasc) as int64)
            end ano_nascimento_paciente,
            case
                when trim(nu_idade_n) = ''
                then null
                when regexp_contains(nu_idade_n, '[^a-zA-Z0-9 ]')
                then null
                else safe_cast(trim(nu_idade_n) as int64)
            end idade_paciente,
            case
                when trim(cs_sexo) = ''
                then null
                when regexp_contains(cs_sexo, '[^a-zA-Z0-9 ]')
                then null
                when trim(cs_sexo) = 'm'
                then 'M'
                else safe_cast(trim(cs_sexo) as string)
            end sexo_paciente,
            case
                when trim(cs_raca) = ''
                then null
                when regexp_contains(cs_raca, '[^a-zA-Z0-9 ]')
                then null
                when trim(cs_raca) = 'H'
                then null
                else safe_cast(trim(cs_raca) as string)
            end raca_cor_paciente,
            case
                when trim(cs_escol_n) = ''
                then null
                when regexp_contains(cs_escol_n, '[^a-zA-Z0-9 ]')
                then null
                else
                    case
                        when trim(regexp_replace(cs_escol_n, r'^0+', '')) = ''
                        then null
                        else
                            safe_cast(
                                trim(regexp_replace(cs_escol_n, r'^0+', '')) as string
                            )
                    end
            end escolaridade_paciente,
            case
                when trim(id_ocupa_n) = ''
                then null
                when regexp_contains(trim(id_ocupa_n), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(id_ocupa_n), '[a-zA-Z]')
                then null
                else safe_cast(trim(id_ocupa_n) as string)
            end ocupacao_paciente,
            case
                when trim(cs_gestant) = ''
                then null
                when regexp_contains(trim(cs_gestant), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(cs_gestant), '[a-zA-Z]')
                then null
                else safe_cast(trim(cs_gestant) as string)
            end gestante_paciente,
            case
                when trim(auto_imune) = ''
                then null
                when regexp_contains(trim(auto_imune), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(auto_imune), '[a-zA-Z]')
                then null
                else safe_cast(trim(auto_imune) as string)
            end possui_doenca_autoimune,
            case
                when trim(diabetes) = ''
                then null
                when regexp_contains(trim(diabetes), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(diabetes), '[a-zA-Z]')
                then null
                else safe_cast(trim(diabetes) as string)
            end possui_diabetes,
            case
                when trim(hematolog) = ''
                then null
                when regexp_contains(trim(hematolog), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(hematolog), '[a-zA-Z]')
                then null
                else safe_cast(trim(hematolog) as string)
            end possui_doencas_hematologicas,
            case
                when trim(hepatopat) = ''
                then null
                when regexp_contains(trim(hepatopat), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(hepatopat), '[a-zA-Z]')
                then null
                else safe_cast(trim(hepatopat) as string)
            end possui_hepatopatias,
            case
                when trim(renal) = ''
                then null
                when regexp_contains(trim(renal), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(renal), '[a-zA-Z]')
                then null
                else safe_cast(trim(renal) as string)
            end possui_doenca_renal,
            case
                when trim(hipertensa) = ''
                then null
                when regexp_contains(trim(hipertensa), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(hipertensa), '[a-zA-Z]')
                then null
                else safe_cast(trim(hipertensa) as string)
            end possui_hipertensao,
            case
                when trim(acido_pept) = ''
                then null
                when regexp_contains(trim(acido_pept), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(acido_pept), '[a-zA-Z]')
                then null
                else safe_cast(trim(acido_pept) as string)
            end possui_doenca_acido_peptica,
            case
                when trim(dt_invest) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_invest, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_investigacao,
            case
                when trim(febre) = ''
                then null
                when regexp_contains(trim(febre), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(febre), '[a-zA-Z]')
                then null
                else safe_cast(trim(febre) as string)
            end apresenta_febre,
            case
                when trim(cefaleia) = ''
                then null
                when trim(cefaleia) not in ('1', '2', '9')
                then null
                when regexp_contains(trim(cefaleia), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(cefaleia), '[a-zA-Z]')
                then null
                else safe_cast(trim(cefaleia) as string)
            end apresenta_cefaleia,
            case
                when trim(exantema) = ''
                then null
                when regexp_contains(trim(exantema), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(exantema), '[a-zA-Z]')
                then null
                else safe_cast(trim(exantema) as string)
            end apresenta_exantema,
            case
                when trim(dor_costas) = ''
                then null
                when regexp_contains(trim(dor_costas), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(dor_costas), '[a-zA-Z]')
                then null
                else safe_cast(trim(dor_costas) as string)
            end apresenta_dor_costas,
            case
                when trim(mialgia) = ''
                then null
                when regexp_contains(trim(mialgia), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(mialgia), '[a-zA-Z]')
                then null
                else safe_cast(trim(mialgia) as string)
            end apresenta_mialgia,
            case
                when trim(vomito) = ''
                then null
                when regexp_contains(trim(vomito), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(vomito), '[a-zA-Z]')
                then null
                else safe_cast(trim(vomito) as string)
            end apresenta_vomito,
            case
                when trim(nausea) = ''
                then null
                when regexp_contains(trim(nausea), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(nausea), '[a-zA-Z]')
                then null
                else safe_cast(trim(nausea) as string)
            end apresenta_nausea,
            case
                when trim(conjuntvit) = ''
                then null
                when trim(conjuntvit) not in ('1', '2', '9')
                then null
                when regexp_contains(trim(conjuntvit), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(conjuntvit), '[a-zA-Z]')
                then null
                else safe_cast(trim(conjuntvit) as string)
            end apresenta_conjutivite,
            case
                when trim(dor_retro) = ''
                then null
                when trim(dor_retro) not in ('1', '2', '9')
                then null
                when regexp_contains(trim(dor_retro), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(dor_retro), '[a-zA-Z]')
                then null
                else safe_cast(trim(dor_retro) as string)
            end apresenta_dor_retroorbital,
            case
                when trim(artralgia) = ''
                then null
                when trim(artralgia) not in ('1', '2', '9')
                then null
                when regexp_contains(trim(artralgia), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(artralgia), '[a-zA-Z]')
                then null
                else safe_cast(trim(artralgia) as string)
            end apresenta_artralgia,
            case
                when trim(artrite) = ''
                then null
                when regexp_contains(trim(artrite), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(artrite), '[a-zA-Z]')
                then null
                else safe_cast(trim(artrite) as string)
            end apresenta_artrite,
            case
                when trim(leucopenia) = ''
                then null
                when trim(leucopenia) not in ('1', '2', '9')
                then null
                when regexp_contains(trim(leucopenia), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(leucopenia), '[a-zA-Z]')
                then null
                else safe_cast(trim(leucopenia) as string)
            end apresenta_leucopenia,
            case
                when trim(petequia_n) = ''
                then null
                when regexp_contains(trim(petequia_n), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(petequia_n), '[a-zA-Z]')
                then null
                else safe_cast(trim(petequia_n) as string)
            end apresenta_petequias,
            case
                when trim(laco) = ''
                then null
                when regexp_contains(trim(laco), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(laco), '[a-zA-Z]')
                then null
                else safe_cast(trim(laco) as string)
            end prova_laco,
            case
                when trim(hospitaliz) = ''
                then null
                when regexp_contains(trim(hospitaliz), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(hospitaliz), '[a-zA-Z]')
                then null
                else safe_cast(trim(hospitaliz) as string)
            end internacao,
            case
                when trim(dt_interna) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_interna, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_internacao,
            case
                when trim(uf) = ''
                then null
                when regexp_contains(trim(uf), '[a-zA-Z]')
                then null
                when regexp_contains(trim(uf), '[^a-zA-Z0-9 ]')
                then null
                when trim(uf) = '11'
                then 'RO'
                when trim(uf) = '12'
                then 'AC'
                when trim(uf) = '13'
                then 'AM'
                when trim(uf) = '14'
                then 'RR'
                when trim(uf) = '15'
                then 'PA'
                when trim(uf) = '16'
                then 'AP'
                when trim(uf) = '17'
                then 'TO'
                when trim(uf) = '21'
                then 'MA'
                when trim(uf) = '22'
                then 'PI'
                when trim(uf) = '23'
                then 'CE'
                when trim(uf) = '24'
                then 'RN'
                when trim(uf) = '25'
                then 'PB'
                when trim(uf) = '26'
                then 'PE'
                when trim(uf) = '27'
                then 'AL'
                when trim(uf) = '28'
                then 'SE'
                when trim(uf) = '29'
                then 'BA'
                when trim(uf) = '31'
                then 'MG'
                when trim(uf) = '32'
                then 'ES'
                when trim(uf) = '33'
                then 'RJ'
                when trim(uf) = '35'
                then 'SP'
                when trim(uf) = '41'
                then 'PR'
                when trim(uf) = '42'
                then 'SC'
                when trim(uf) = '43'
                then 'RS'
                when trim(uf) = '50'
                then 'MS'
                when trim(uf) = '51'
                then 'MT'
                when trim(uf) = '52'
                then 'GO'
                when trim(uf) = '53'
                then 'DF'
                else safe_cast(trim(uf) as string)
            end sigla_uf_internacao,
            case
                when trim(municipio) = ''
                then null
                when regexp_contains(trim(municipio), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(municipio), '[a-zA-Z]')
                then null
                else safe_cast(trim(municipio) as string)
            end id_municipio_internacao,
            case
                when trim(dt_chik_s1) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_chik_s1, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sorologia1_chikungunya,
            case
                when trim(res_chiks1) = ''
                then null
                when regexp_contains(trim(res_chiks1), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(res_chiks1), '[a-zA-Z]')
                then null
                else safe_cast(trim(res_chiks1) as string)
            end resultado_sorologia1_chikungunya,
            case
                when trim(res_chiks2) = ''
                then null
                when regexp_contains(trim(res_chiks2), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(res_chiks2), '[a-zA-Z]')
                then null
                else safe_cast(trim(res_chiks2) as string)
            end resultado_sorologia2_chikungunya,
            case
                when trim(resul_prnt) = ''
                then null
                when regexp_contains(trim(resul_prnt), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(resul_prnt), '[a-zA-Z]')
                then null
                else safe_cast(trim(resul_prnt) as string)
            end resultado_prnt,
            case
                when trim(dt_ns1) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_ns1, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_ns1,
            case
                when trim(resul_ns1) = ''
                then null
                when regexp_contains(trim(resul_ns1), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(resul_ns1), '[a-zA-Z]')
                then null
                else safe_cast(trim(resul_ns1) as string)
            end resultado_ns1,
            case
                when trim(dt_viral) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_viral, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_viral,
            case
                when trim(resul_vi_n) = ''
                then null
                when regexp_contains(trim(resul_vi_n), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(resul_vi_n), '[a-zA-Z]')
                then null
                else safe_cast(trim(resul_vi_n) as string)
            end resultado_viral,
            case
                when trim(dt_pcr) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_pcr, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_pcr,
            case
                when trim(resul_pcr_) = ''
                then null
                when regexp_contains(trim(resul_pcr_), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(resul_pcr_), '[a-zA-Z]')
                then null
                else safe_cast(trim(resul_pcr_) as string)
            end resultado_pcr,
            case
                when trim(dt_soro) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_soro, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sorologia_dengue,
            case
                when trim(resul_soro) = ''
                then null
                when regexp_contains(trim(resul_soro), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(resul_soro), '[a-zA-Z]')
                then null
                else safe_cast(trim(resul_soro) as string)
            end resultado_sorologia_dengue,
            case
                when trim(sorotipo) = ''
                then null
                when regexp_contains(trim(sorotipo), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(sorotipo), '[a-zA-Z]')
                then null
                else safe_cast(trim(sorotipo) as string)
            end sorotipo,
            case
                when trim(histopa_n) = ''
                then null
                when regexp_contains(trim(histopa_n), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(histopa_n), '[a-zA-Z]')
                then null
                else safe_cast(trim(histopa_n) as string)
            end histopatologia,
            case
                when trim(imunoh_n) = ''
                then null
                when regexp_contains(trim(imunoh_n), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(imunoh_n), '[a-zA-Z]')
                then null
                else safe_cast(trim(imunoh_n) as string)
            end imunohistoquimica,
            case
                when trim(classi_fin) = ''
                then null
                when trim(classi_fin) not in ('5', '10', '11', '12', '13')
                then null
                when regexp_contains(trim(classi_fin), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(classi_fin), '[a-zA-Z]')
                then null
                else safe_cast(trim(classi_fin) as string)
            end classificacao_final,
            case
                when trim(criterio) = ''
                then null
                when regexp_contains(trim(criterio), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(criterio), '[a-zA-Z]')
                then null
                else safe_cast(trim(criterio) as string)
            end criterio_confirmacao,
            case
                when trim(tpautocto) = ''
                then null
                when regexp_contains(trim(tpautocto), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(tpautocto), '[a-zA-Z]')
                then null
                else safe_cast(trim(tpautocto) as string)
            end caso_autoctone,
            case
                when trim(copaisinf) = ''
                then null
                when regexp_contains(trim(copaisinf), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(copaisinf), '[a-zA-Z]')
                then null
                else safe_cast(trim(copaisinf) as string)
            end pais_infeccao,
            case
                when trim(coufinf) = ''
                then null
                when regexp_contains(trim(coufinf), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(coufinf), '[a-zA-Z]')
                then null
                when trim(coufinf) = '11'
                then 'RO'
                when trim(coufinf) = '12'
                then 'AC'
                when trim(coufinf) = '13'
                then 'AM'
                when trim(coufinf) = '14'
                then 'RR'
                when trim(coufinf) = '15'
                then 'PA'
                when trim(coufinf) = '16'
                then 'AP'
                when trim(coufinf) = '17'
                then 'TO'
                when trim(coufinf) = '21'
                then 'MA'
                when trim(coufinf) = '22'
                then 'PI'
                when trim(coufinf) = '23'
                then 'CE'
                when trim(coufinf) = '24'
                then 'RN'
                when trim(coufinf) = '25'
                then 'PB'
                when trim(coufinf) = '26'
                then 'PE'
                when trim(coufinf) = '27'
                then 'AL'
                when trim(coufinf) = '28'
                then 'SE'
                when trim(coufinf) = '29'
                then 'BA'
                when trim(coufinf) = '31'
                then 'MG'
                when trim(coufinf) = '32'
                then 'ES'
                when trim(coufinf) = '33'
                then 'RJ'
                when trim(coufinf) = '35'
                then 'SP'
                when trim(coufinf) = '41'
                then 'PR'
                when trim(coufinf) = '42'
                then 'SC'
                when trim(coufinf) = '43'
                then 'RS'
                when trim(coufinf) = '50'
                then 'MS'
                when trim(coufinf) = '51'
                then 'MT'
                when trim(coufinf) = '52'
                then 'GO'
                when trim(coufinf) = '53'
                then 'DF'
                else safe_cast(trim(coufinf) as string)
            end sigla_uf_infeccao,
            case
                when trim(comuninf) = ''
                then null
                when regexp_contains(trim(comuninf), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(comuninf), '[a-zA-Z]')
                then null
                else safe_cast(trim(comuninf) as string)
            end id_municipio_infeccao,
            case
                when trim(doenca_tra) = ''
                then null
                when regexp_contains(trim(doenca_tra), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(doenca_tra), '[a-zA-Z]')
                then null
                else safe_cast(trim(doenca_tra) as string)
            end doenca_trabalho,
            case
                when trim(clinc_chik) = ''
                then null
                when regexp_contains(trim(clinc_chik), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(clinc_chik), '[a-zA-Z]')
                then null
                else safe_cast(trim(clinc_chik) as string)
            end apresentacao_clinica,
            case
                when trim(evolucao) = ''
                then null
                when regexp_contains(trim(evolucao), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(evolucao), '[a-zA-Z]')
                then null
                else safe_cast(trim(evolucao) as string)
            end evolucao_caso,
            case
                when trim(dt_obito) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_obito, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_obito,
            case
                when trim(dt_encerra) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_encerra, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_encerramento,
            case
                when trim(tp_sistema) = ''
                then null
                when regexp_contains(trim(tp_sistema), '[^a-zA-Z0-9 ]')
                then null
                when regexp_contains(trim(tp_sistema), '[a-zA-Z]')
                then null
                else safe_cast(trim(tp_sistema) as string)
            end tipo_sistema,
            case
                when trim(dt_digita) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_digita, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_digitacao,
            case
                when trim(dt_chik_s2) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_chik_s2, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sorologia2_chikungunya,
            case
                when trim(dt_prnt) = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_prnt, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_coleta_prnt
        from `basedosdados-staging.br_ms_sinan_staging.microdados_chikungunya`
    )

select
    ano,
    tipo_notificacao,
    id_agravo,
    case
        when extract(year from data_notificacao) > extract(year from current_date)
        then null
        else data_notificacao
    end data_notificacao,
    semana_notificacao,
    sigla_uf_notificacao,
    id_regional_saude_notificacao,
    id_municipio_notificacao,
    id_estabelecimento,
    case
        when
            extract(year from data_primeiros_sintomas) > extract(year from current_date)
        then null
        else data_primeiros_sintomas
    end data_primeiros_sintomas,
    semana_sintomas,
    pais_residencia,
    sigla_uf_residencia,
    id_regional_saude_residencia,
    id_municipio_residencia,
    ano_nascimento_paciente,
    idade_paciente,
    sexo_paciente,
    raca_cor_paciente,
    escolaridade_paciente,
    ocupacao_paciente,
    gestante_paciente,
    possui_doenca_autoimune,
    possui_diabetes,
    possui_doencas_hematologicas,
    possui_hepatopatias,
    possui_doenca_renal,
    possui_hipertensao,
    possui_doenca_acido_peptica,
    case
        when extract(year from data_investigacao) > extract(year from current_date)
        then null
        else data_investigacao
    end data_investigacao,
    apresenta_febre,
    apresenta_cefaleia,
    apresenta_exantema,
    apresenta_dor_costas,
    apresenta_mialgia,
    apresenta_vomito,
    apresenta_nausea,
    apresenta_conjutivite,
    apresenta_dor_retroorbital,
    apresenta_artralgia,
    apresenta_artrite,
    apresenta_leucopenia,
    apresenta_petequias,
    prova_laco,
    internacao,
    case
        when extract(year from data_internacao) > extract(year from current_date)
        then null
        else data_internacao
    end data_internacao,
    sigla_uf_internacao,
    id_municipio_internacao,
    case
        when
            extract(year from data_sorologia1_chikungunya)
            > extract(year from current_date)
        then null
        else data_sorologia1_chikungunya
    end data_sorologia1_chikungunya,
    resultado_sorologia1_chikungunya,
    case
        when
            extract(year from data_sorologia2_chikungunya)
            > extract(year from current_date)
        then null
        else data_sorologia2_chikungunya
    end data_sorologia2_chikungunya,
    resultado_sorologia2_chikungunya,
    case
        when extract(year from data_coleta_prnt) > extract(year from current_date)
        then null
        else data_coleta_prnt
    end data_coleta_prnt,
    resultado_prnt,
    case
        when extract(year from data_ns1) > extract(year from current_date)
        then null
        else data_ns1
    end data_ns1,
    resultado_ns1,
    case
        when extract(year from data_viral) > extract(year from current_date)
        then null
        else data_viral
    end data_viral,
    resultado_viral,
    case
        when extract(year from data_pcr) > extract(year from current_date)
        then null
        else data_pcr
    end data_pcr,
    resultado_pcr,
    case
        when extract(year from data_sorologia_dengue) > extract(year from current_date)
        then null
        else data_sorologia_dengue
    end data_sorologia_dengue,
    resultado_sorologia_dengue,
    sorotipo,
    histopatologia,
    imunohistoquimica,
    classificacao_final,
    criterio_confirmacao,
    caso_autoctone,
    pais_infeccao,
    sigla_uf_infeccao,
    id_municipio_infeccao,
    doenca_trabalho,
    apresentacao_clinica,
    evolucao_caso,
    case
        when extract(year from data_obito) > extract(year from current_date)
        then null
        else data_obito
    end data_obito,
    case
        when extract(year from data_encerramento) > extract(year from current_date)
        then null
        else data_encerramento
    end data_encerramento,
    case
        when extract(year from data_digitacao) > extract(year from current_date)
        then null
        else data_digitacao
    end data_digitacao,
    tipo_sistema
from sql_chik
