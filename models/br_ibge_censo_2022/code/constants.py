from enum import Enum

class constants(Enum):
    URLS = {"populacao_grupos_idade":"https://apisidra.ibge.gov.br/values/t/1209/n3/all/v/allxp/p/all/c58/all",
        "populacao_residente": "https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/all/c287/allxt/c286/allxt",
        "indice_envelhecimento": "https://apisidra.ibge.gov.br/values/t/9515/n3/all/n6/all/v/all/p/all/d/v8845%202,v10612%202",
        "populacao_residente_variacao_absoluta": "https://apisidra.ibge.gov.br/values/t/4709/n6/all/v/all/p/all/d/v10605%202",
        "area_territorial_densidade_demografica": "https://apisidra.ibge.gov.br/values/t/4714/n6/all/v/all/p/all/d/v614%202",
        "domicilios_recenseados_especie": "https://apisidra.ibge.gov.br/values/t/4711/n6/all/v/allxp/p/all/c3/allxt",
        "domicilios_moradores": "https://apisidra.ibge.gov.br/values/t/4712/n6/all/v/all/p/all/d/v5930%202",
        "populacao_residente_quilombola": "https://apisidra.ibge.gov.br/values/t/9578/n6/all/v/93,4709/p/all/c2661/allxt",
        "domicilios_moradores_quilombolas": "https://apisidra.ibge.gov.br/values/t/9727/n6/all/v/381,382,5930,7085,7097/p/all/c2661/allxt/d/v5930%202,v7085%202",
        "domicilios_pelo_menos_um_morador_quilombola": "https://apisidra.ibge.gov.br/values/t/9724/n6/all/v/7081,7082,7083,7084,7085/p/all/c2661/allxt/d/v7084%202,v7085%202",
        "populacao_residente_territorios_quilombolas": "https://apisidra.ibge.gov.br/values/t/9723/n145/all/v/6559,7079/p/all",
        "domicilios_moradores_territorios_quilombolas" : "https://apisidra.ibge.gov.br/values/t/9725/n145/all/v/7086,7087,7091,7095,7096/p/all/d/v7095%202,v7096%202",
        "domicilios_pelo_menos_um_territorios_quilombolas": "https://apisidra.ibge.gov.br/values/t/9726/n145/all/v/7081,7082,7083,7084,7085/p/all/d/v7084%202,v7085%202",
        "populacao_residente_indigena_municipio": "https://apisidra.ibge.gov.br/values/t/9718/n6/all/v/93,350/p/all/c1714/allxt/c2661/allxt",
        "domicilios_moradores_indigenas_municipio": "https://apisidra.ibge.gov.br/values/t/9728/n6/all/v/381,382,5930,6554,8691/p/all/c2661/allxt/d/v5930%202,v6554%202",
        "domicilios_pelo_menos_um_morador_indigenas_municipio": "https://apisidra.ibge.gov.br/values/t/9720/n6/all/v/5938,6554,7088,7089,7090/p/all/c2661/allxt/d/v5938%202,v6554%202" }