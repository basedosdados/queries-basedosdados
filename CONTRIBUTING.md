# 1. Garanta que você já tem em mãos:
- o nome do conjunto e da tabela validada pela equipe dados da BD
- tabela de arquitetura validada pela equipe dados em um google_sheets com a permissão de edição para qualquer um com o link
- arquivo dos dados no formato .csv ou .parquet (pode ser em hive ou não) 
- código `download_data.py`
- código `clean_data.py` (se necessário)
- chave de acesso ao google cloud dos voluntários

# 2. Configurações para subir os dados e rodar o modelo DBT:

* Clonar o diretório `queries-basedosdados-dev` 

* Acessar o diretório `queries-basedosdados-dev`
```
cd <path/do/seu/clone/queries-basedosdados-dev>
```

* Atualizar o `pip` 
É muito importante garantir que a versão do pip está atualizada antes dos próximos passos.
```bash
python -m pip install --upgrade pip
```
* Instalar o poetry
```bash
pip install poetry
```

* Criar o ambiente virtual e instalar as dependências 
```bash
poetry install --with=dev && pre-commit install
```
* Ative o ambiente virtual
```bash
poetry shell
```
* Instalar os pacotes do dbt no ambiente local
```bash
dbt deps
```
## Alterar as credenciais em `profiles.yml`:
Para que o teste dos modelos seja feito localmente é necessário mudar o caminho das credenciais que ficam no arquivo `profiles.yml`.
* Criar uma cópia do arquivo `profiles.yml` original fora do repo `queries-basedosdados-dev`,
* No campo `keyfile` altera o caminho para a credencial local (se vc já usou o pacote `basedosdados` ela deve estar em `/home/<user>/.basedosdados/credentials/prod.json`)

Pronto! agora você já tem seu ambiente configurado para desenvolver e rodar os modelos no seu ambiente local.

# 3. Subir os dados brutos no BigQuery e gerar aquivos DBT

A gente criou um arquivo .py pra você seguir os passos ele se encontra em `queries-basedosdados-dev/gist/upload_data_and_create_dbt_files.py` mas segue aqui a explicação de cada bloco em português:

Definimos as variáveis para identificar o conjunto de dados, tabela, URL da arquitetura e caminho para os dados
```python 
    dataset_id = '<dataset_id>'
    table_id = '<table_id>'
    architecture_url = '<url_from_google_sheets>' #lembre de que o link deve ser compartilhavel para qualquer pessoa na internet editar
    path_to_data = f"/path_to_datasets/{dataset_id}/{table_id}"  
```

Criamos um objeto do tipo Table da bd: 
```python 
    tb = bd.Table(
        dataset_id=dataset_id,
        table_id=table_id
    )
``` 

No próximo passo carregamos os dados para o Storage da BD e criamos uma tabela BigQuery que por uma conexão externa acessa esses dados diretamente do Storage
> Deixamos os parâmetros que mais usamos visíveis aqui, mas é importante explorar outras opções de parâmetros na documentação.
   
```python 
    tb.create(
        path=path_to_data,
        if_storage_data_exists='raise',
        if_table_exists='replace',
        source_format='csv'
    )
``` 
Essa função cria os arquivos padrão necessários para executar um modelo dbt 
> Atenção: ainda será necessário realizar modificações nos arquivos
```python 
    create_yaml_file(
        arq_url=architecture_url,
        table_id=table_id,
        dataset_id=dataset_id,
        preprocessed_staging_column_names=False) 
        # Mude esta última variável para True se 'original_name' foi modificado para 'name' no script de limpeza

```

# 4. Transformação dos arquivos e verificando o resultado
Primeiro vamos criar uma branch para subir esses novos arquivos. O nome padrão que usamos na bd para as branchs é apenas `<dataset_id>`

Agora será necessário fazer as seguintes modificações:
### no arquivo `<dataset_id>__<table_id>.sql`
* Incluir colunas que sejam modificações das colunas originais (como por exemplo puxar o id_municipio a partir do nome)
* Modificar colunas que sejam do tipo string que contenham códigos númericos para excluir '.0' ao final da string
* Caso a coluna seja do tipo `date` avaliar como usar as funções do SQL para que ela fique registrada no tipo correto
* Outras transformações necessárias para que ao final do processo a tabela gerada com o código SQL seja igual a arquitetura
> Obs. Para realizar testes pode rodar no próprio console do BigQuery, quando tudo estiver pronto você copia e cola a querie no arquivo `.sql` 

### no arquivo `schema.yaml`
* Incluir as colunas necessárias no teste de chave única   
* Incluir mais testes para verificar se o dado está se comportando como deveria, temos alguns testes pré prontos com o pacote de [dbt-utils](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#generic-tests)

### no arquivo `dbt_project.yml`
* Caso seja um conjunto novo, incluir o conjunto na lista do projeto **em ordem alfabética**

## Como rodar o modelo?
Mais informações: https://docs.getdbt.com/reference/node-selection/syntax
### Materializando o modelo no BigQuery:
```bash
# materializa um único modelo pelo nome
$ dbt run --select dataset_id__table_id --profiles-dir <path/to/profiles.yml>     

# materializa todos os modelos em uma pasta      
$ dbt run --select model.dateset_id.dateset_id__table_id --profiles-dir <path/to/profiles.yml>      

# materializa todos os modelos no caminho 
$ dbt run --select /models/dataset_id --profiles-dir <path/to/profiles.yml>
 
# materializa um único modelo pelo caminho sql
$ dbt run --select /models/dataset/table_id.sql --profiles-dir <path/to/profiles.yml> 
```

### Testando o modelo pelas configurações do `schema.yaml` 
```bash
# materializa um único modelo pelo nome
$ dbt test --select dataset_id__table_id --profiles-dir <path/to/profiles.yml>     

# materializa todos os modelos em uma pasta      
$ dbt test --select model.dateset_id.dateset_id__table_id --profiles-dir <path/to/profiles.yml>      

# materializa todos os modelos no caminho 
$ dbt test --select /models/dataset_id --profiles-dir <path/to/profiles.yml>
 
```
* **Obs:** Como os testes ficam no arquivo `schema.yml` não é possível passar o caminho do `.sql` para realizar testes
