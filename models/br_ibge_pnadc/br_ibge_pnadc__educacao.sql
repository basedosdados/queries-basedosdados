{{
    config(
        alias="educacao",
        schema="br_ibge_pnadc",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2025, "interval": 1},
        },
        cluster_by="sigla_uf",
        labels={"project_id": "basedosdados"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(trimestre as int64) trimestre,
    safe_cast(id_uf as string) id_uf,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(capital as string) capital,
    safe_cast(rm_ride as string) rm_ride,
    safe_cast(id_upa as string) id_upa,
    safe_cast(id_estrato as string) id_estrato,
    safe_cast(id_domicilio as string) id_domicilio,
    safe_cast(id_pessoa as string) id_pessoa,
    safe_cast(v1008 as string) v1008,
    safe_cast(v1014 as string) v1014,
    safe_cast(v1016 as int64) v1016,
    safe_cast(v1022 as string) v1022,
    safe_cast(v1023 as string) v1023,
    safe_cast(v1027 as float64) v1027,
    safe_cast(v1028 as float64) v1028,
    safe_cast(v1029 as int64) v1029,
    safe_cast(v1033 as int64) v1033,
    safe_cast(posest as string) posest,
    safe_cast(posest_sxi as string) posest_sxi,
    safe_cast(v2001 as int64) v2001,
    safe_cast(v2003 as int64) v2003,
    safe_cast(v3001 as string) v3001,
    safe_cast(v3002 as string) v3002,
    safe_cast(v3002a as string) v3002a,
    safe_cast(v3003a as string) v3003a,
    safe_cast(v3004 as string) v3004,
    safe_cast(v3004a as string) v3004a,
    safe_cast(v3005a as string) v3005a,
    safe_cast(v3006 as string) v3006,
    safe_cast(v3006a as string) v3006a,
    safe_cast(v3006b as string) v3006b,
    safe_cast(v3006c as string) v3006c,
    safe_cast(v3007 as string) v3007,
    safe_cast(v3008 as string) v3008,
    safe_cast(v3009a as string) v3009a,
    safe_cast(v3010 as string) v3010,
    safe_cast(v3010a as string) v3010a,
    safe_cast(v3011a as string) v3011a,
    safe_cast(v3012 as string) v3012,
    safe_cast(v3013 as string) v3013,
    safe_cast(v3013a as string) v3013a,
    safe_cast(v3013b as string) v3013b,
    safe_cast(v3014 as string) v3014,
    safe_cast(v3017 as string) v3017,
    safe_cast(v3017a as string) v3017a,
    safe_cast(v3018 as string) v3018,
    safe_cast(v3019 as string) v3019,
    safe_cast(v3019a as string) v3019a,
    safe_cast(v3020 as string) v3020,
    safe_cast(v3020b as string) v3020b,
    safe_cast(v3020c as string) v3020c,
    safe_cast(v3021 as string) v3021,
    safe_cast(v3021a as string) v3021a,
    safe_cast(v3022 as string) v3022,
    safe_cast(v3022a as string) v3022a,
    safe_cast(v3022c as string) v3022c,
    safe_cast(v3022d as string) v3022d,
    safe_cast(v3022e as string) v3022e,
    safe_cast(v3023 as string) v3023,
    safe_cast(v3023a as string) v3023a,
    safe_cast(v3024 as string) v3024,
    safe_cast(v3025 as string) v3025,
    safe_cast(v3026 as string) v3026,
    safe_cast(v3026a as string) v3026a,
    safe_cast(v3028 as string) v3028,
    safe_cast(v3029 as string) v3029,
    safe_cast(v3029a as string) v3029a,
    safe_cast(v3030 as string) v3030,
    safe_cast(v3030a as string) v3030a,
    safe_cast(v3032 as string) v3032,
    safe_cast(v3033 as string) v3033,
    safe_cast(v3033a as string) v3033a,
    safe_cast(v3033b as string) v3033b,
    safe_cast(v3034 as string) v3034,
    safe_cast(v3034a as string) v3034a,
    safe_cast(v3034b as string) v3034b,
    safe_cast(v3034c as string) v3034c,
    safe_cast(v1028001 as float64) v1028001,
    safe_cast(v1028002 as float64) v1028002,
    safe_cast(v1028003 as float64) v1028003,
    safe_cast(v1028004 as float64) v1028004,
    safe_cast(v1028005 as float64) v1028005,
    safe_cast(v1028006 as float64) v1028006,
    safe_cast(v1028007 as float64) v1028007,
    safe_cast(v1028008 as float64) v1028008,
    safe_cast(v1028009 as float64) v1028009,
    safe_cast(v1028010 as float64) v1028010,
    safe_cast(v1028011 as float64) v1028011,
    safe_cast(v1028012 as float64) v1028012,
    safe_cast(v1028013 as float64) v1028013,
    safe_cast(v1028014 as float64) v1028014,
    safe_cast(v1028015 as float64) v1028015,
    safe_cast(v1028016 as float64) v1028016,
    safe_cast(v1028017 as float64) v1028017,
    safe_cast(v1028018 as float64) v1028018,
    safe_cast(v1028019 as float64) v1028019,
    safe_cast(v1028020 as float64) v1028020,
    safe_cast(v1028021 as float64) v1028021,
    safe_cast(v1028022 as float64) v1028022,
    safe_cast(v1028023 as float64) v1028023,
    safe_cast(v1028024 as float64) v1028024,
    safe_cast(v1028025 as float64) v1028025,
    safe_cast(v1028026 as float64) v1028026,
    safe_cast(v1028027 as float64) v1028027,
    safe_cast(v1028028 as float64) v1028028,
    safe_cast(v1028029 as float64) v1028029,
    safe_cast(v1028030 as float64) v1028030,
    safe_cast(v1028031 as float64) v1028031,
    safe_cast(v1028032 as float64) v1028032,
    safe_cast(v1028033 as float64) v1028033,
    safe_cast(v1028034 as float64) v1028034,
    safe_cast(v1028035 as float64) v1028035,
    safe_cast(v1028036 as float64) v1028036,
    safe_cast(v1028037 as float64) v1028037,
    safe_cast(v1028038 as float64) v1028038,
    safe_cast(v1028039 as float64) v1028039,
    safe_cast(v1028040 as float64) v1028040,
    safe_cast(v1028041 as float64) v1028041,
    safe_cast(v1028042 as float64) v1028042,
    safe_cast(v1028043 as float64) v1028043,
    safe_cast(v1028044 as float64) v1028044,
    safe_cast(v1028045 as float64) v1028045,
    safe_cast(v1028046 as float64) v1028046,
    safe_cast(v1028047 as float64) v1028047,
    safe_cast(v1028048 as float64) v1028048,
    safe_cast(v1028049 as float64) v1028049,
    safe_cast(v1028050 as float64) v1028050,
    safe_cast(v1028051 as float64) v1028051,
    safe_cast(v1028052 as float64) v1028052,
    safe_cast(v1028053 as float64) v1028053,
    safe_cast(v1028054 as float64) v1028054,
    safe_cast(v1028055 as float64) v1028055,
    safe_cast(v1028056 as float64) v1028056,
    safe_cast(v1028057 as float64) v1028057,
    safe_cast(v1028058 as float64) v1028058,
    safe_cast(v1028059 as float64) v1028059,
    safe_cast(v1028060 as float64) v1028060,
    safe_cast(v1028061 as float64) v1028061,
    safe_cast(v1028062 as float64) v1028062,
    safe_cast(v1028063 as float64) v1028063,
    safe_cast(v1028064 as float64) v1028064,
    safe_cast(v1028065 as float64) v1028065,
    safe_cast(v1028066 as float64) v1028066,
    safe_cast(v1028067 as float64) v1028067,
    safe_cast(v1028068 as float64) v1028068,
    safe_cast(v1028069 as float64) v1028069,
    safe_cast(v1028070 as float64) v1028070,
    safe_cast(v1028071 as float64) v1028071,
    safe_cast(v1028072 as float64) v1028072,
    safe_cast(v1028073 as float64) v1028073,
    safe_cast(v1028074 as float64) v1028074,
    safe_cast(v1028075 as float64) v1028075,
    safe_cast(v1028076 as float64) v1028076,
    safe_cast(v1028077 as float64) v1028077,
    safe_cast(v1028078 as float64) v1028078,
    safe_cast(v1028079 as float64) v1028079,
    safe_cast(v1028080 as float64) v1028080,
    safe_cast(v1028081 as float64) v1028081,
    safe_cast(v1028082 as float64) v1028082,
    safe_cast(v1028083 as float64) v1028083,
    safe_cast(v1028084 as float64) v1028084,
    safe_cast(v1028085 as float64) v1028085,
    safe_cast(v1028086 as float64) v1028086,
    safe_cast(v1028087 as float64) v1028087,
    safe_cast(v1028088 as float64) v1028088,
    safe_cast(v1028089 as float64) v1028089,
    safe_cast(v1028090 as float64) v1028090,
    safe_cast(v1028091 as float64) v1028091,
    safe_cast(v1028092 as float64) v1028092,
    safe_cast(v1028093 as float64) v1028093,
    safe_cast(v1028094 as float64) v1028094,
    safe_cast(v1028095 as float64) v1028095,
    safe_cast(v1028096 as float64) v1028096,
    safe_cast(v1028097 as float64) v1028097,
    safe_cast(v1028098 as float64) v1028098,
    safe_cast(v1028099 as float64) v1028099,
    safe_cast(v1028100 as float64) v1028100,
    safe_cast(v1028101 as float64) v1028101,
    safe_cast(v1028102 as float64) v1028102,
    safe_cast(v1028103 as float64) v1028103,
    safe_cast(v1028104 as float64) v1028104,
    safe_cast(v1028105 as float64) v1028105,
    safe_cast(v1028106 as float64) v1028106,
    safe_cast(v1028107 as float64) v1028107,
    safe_cast(v1028108 as float64) v1028108,
    safe_cast(v1028109 as float64) v1028109,
    safe_cast(v1028110 as float64) v1028110,
    safe_cast(v1028111 as float64) v1028111,
    safe_cast(v1028112 as float64) v1028112,
    safe_cast(v1028113 as float64) v1028113,
    safe_cast(v1028114 as float64) v1028114,
    safe_cast(v1028115 as float64) v1028115,
    safe_cast(v1028116 as float64) v1028116,
    safe_cast(v1028117 as float64) v1028117,
    safe_cast(v1028118 as float64) v1028118,
    safe_cast(v1028119 as float64) v1028119,
    safe_cast(v1028120 as float64) v1028120,
    safe_cast(v1028121 as float64) v1028121,
    safe_cast(v1028122 as float64) v1028122,
    safe_cast(v1028123 as float64) v1028123,
    safe_cast(v1028124 as float64) v1028124,
    safe_cast(v1028125 as float64) v1028125,
    safe_cast(v1028126 as float64) v1028126,
    safe_cast(v1028127 as float64) v1028127,
    safe_cast(v1028128 as float64) v1028128,
    safe_cast(v1028129 as float64) v1028129,
    safe_cast(v1028130 as float64) v1028130,
    safe_cast(v1028131 as float64) v1028131,
    safe_cast(v1028132 as float64) v1028132,
    safe_cast(v1028133 as float64) v1028133,
    safe_cast(v1028134 as float64) v1028134,
    safe_cast(v1028135 as float64) v1028135,
    safe_cast(v1028136 as float64) v1028136,
    safe_cast(v1028137 as float64) v1028137,
    safe_cast(v1028138 as float64) v1028138,
    safe_cast(v1028139 as float64) v1028139,
    safe_cast(v1028140 as float64) v1028140,
    safe_cast(v1028141 as float64) v1028141,
    safe_cast(v1028142 as float64) v1028142,
    safe_cast(v1028143 as float64) v1028143,
    safe_cast(v1028144 as float64) v1028144,
    safe_cast(v1028145 as float64) v1028145,
    safe_cast(v1028146 as float64) v1028146,
    safe_cast(v1028147 as float64) v1028147,
    safe_cast(v1028148 as float64) v1028148,
    safe_cast(v1028149 as float64) v1028149,
    safe_cast(v1028150 as float64) v1028150,
    safe_cast(v1028151 as float64) v1028151,
    safe_cast(v1028152 as float64) v1028152,
    safe_cast(v1028153 as float64) v1028153,
    safe_cast(v1028154 as float64) v1028154,
    safe_cast(v1028155 as float64) v1028155,
    safe_cast(v1028156 as float64) v1028156,
    safe_cast(v1028157 as float64) v1028157,
    safe_cast(v1028158 as float64) v1028158,
    safe_cast(v1028159 as float64) v1028159,
    safe_cast(v1028160 as float64) v1028160,
    safe_cast(v1028161 as float64) v1028161,
    safe_cast(v1028162 as float64) v1028162,
    safe_cast(v1028163 as float64) v1028163,
    safe_cast(v1028164 as float64) v1028164,
    safe_cast(v1028165 as float64) v1028165,
    safe_cast(v1028166 as float64) v1028166,
    safe_cast(v1028167 as float64) v1028167,
    safe_cast(v1028168 as float64) v1028168,
    safe_cast(v1028169 as float64) v1028169,
    safe_cast(v1028170 as float64) v1028170,
    safe_cast(v1028171 as float64) v1028171,
    safe_cast(v1028172 as float64) v1028172,
    safe_cast(v1028173 as float64) v1028173,
    safe_cast(v1028174 as float64) v1028174,
    safe_cast(v1028175 as float64) v1028175,
    safe_cast(v1028176 as float64) v1028176,
    safe_cast(v1028177 as float64) v1028177,
    safe_cast(v1028178 as float64) v1028178,
    safe_cast(v1028179 as float64) v1028179,
    safe_cast(v1028180 as float64) v1028180,
    safe_cast(v1028181 as float64) v1028181,
    safe_cast(v1028182 as float64) v1028182,
    safe_cast(v1028183 as float64) v1028183,
    safe_cast(v1028184 as float64) v1028184,
    safe_cast(v1028185 as float64) v1028185,
    safe_cast(v1028186 as float64) v1028186,
    safe_cast(v1028187 as float64) v1028187,
    safe_cast(v1028188 as float64) v1028188,
    safe_cast(v1028189 as float64) v1028189,
    safe_cast(v1028190 as float64) v1028190,
    safe_cast(v1028191 as float64) v1028191,
    safe_cast(v1028192 as float64) v1028192,
    safe_cast(v1028193 as float64) v1028193,
    safe_cast(v1028194 as float64) v1028194,
    safe_cast(v1028195 as float64) v1028195,
    safe_cast(v1028196 as float64) v1028196,
    safe_cast(v1028197 as float64) v1028197,
    safe_cast(v1028198 as float64) v1028198,
    safe_cast(v1028199 as float64) v1028199,
    safe_cast(v1028200 as float64) v1028200,
from `basedosdados-staging.br_ibge_pnadc_staging.educacao` as t