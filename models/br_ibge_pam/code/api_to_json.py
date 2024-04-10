import asyncio
import aiohttp
import json
import glob
from tqdm.asyncio import tqdm
from aiohttp import ClientTimeout, TCPConnector
from tqdm import tqdm

API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO            = "1613" # É a tabela no SIDRA
PERIODOS            = range(1974, 2022 + 1)
VARIAVEIS           = ["2313", "1002313", "216", "1000216", "214", "112","215",
                       "1000215"] # As variáveis da tabela
NIVEL_GEOGRAFICO    = "N6" # N6 = Municipal
LOCALIDADES         = "all"
CLASSIFICACAO       = "82" # Código pré-definido por agregado
CATEGORIAS          = ["2717", "2718", "45981", "2719", "2720", "2721", "40472",
                       "2722", "2723", "31619", "31620", "40473", "2724", "2725",
                       "2726", "2727", "2728", "2729", "2730", "2731", "2732",
                       "2733", "2734", "2735", "2736", "2737", "2738", "2739",
                       "2740", "90001", "2741", "2742", "2743", "2744", "2745",
                       "2746", "2747", "2748"] # Produtos
ANOS_BAIXADOS       = [int(glob.os.path.basename(f).split(".")[0]) for f in glob.glob(f"../json/*.json")]
ANOS_RESTANTES      = [int(ANO) for ANO in PERIODOS if ANO not in ANOS_BAIXADOS]

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

async def main(years, variables, categories):
    for year in years:
        print(f'Consultando dados do ano: {year}')
        async with aiohttp.ClientSession(connector=TCPConnector(limit=100, force_close=True), timeout=ClientTimeout(total=1200)) as session:
            tasks = []
            for variable in variables: # Foi preciso iterar por cada variável porque senão retorna HTTP 500
                for category in categories:
                    url = API_URL_BASE.format(AGREGADO, year, variable, NIVEL_GEOGRAFICO, LOCALIDADES, CLASSIFICACAO, category)
                    task = fetch(session, url)
                    tasks.append(asyncio.ensure_future(task))
            responses = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                try:
                    response = await future
                    responses.append(response)
                except asyncio.TimeoutError:
                    print(f"Request timed out for {url}")
            with open(f'../json/{year}.json', 'a') as f:
                json.dump(responses, f)

if __name__ == "__main__":
    asyncio.run(main(ANOS_RESTANTES, VARIAVEIS, CATEGORIAS))
