# Metabase

Ferramenta de visualização de dados, que vai utilizar o DuckDB como engine de consulta. Aqui usaremos o docker para executar o processo e para configuração realizamos as seguintes etapas:

1. Devemos buildar a imagem do Metabase, para isso, execute o comando:

    ```bash
        docker build metabase/ --tag metaduck:latest
    ```

2. Depois disso, devemos executar o comando:

    ```bash
        docker run -d --name metaduck -d -p 3000:3000 -v ./duckdb/:/duckdb/ -m 2GB metaduck
    ```

3. Para visualização dos logs, é possível executar o comando: `docker logs -f metaduck`

4. Para acesso ao Metabase, basta acessar o endereço `http://localhost:3000` e realizar o login com as credenciais padrão.

## Detalhes Adicionais

### Path DuckDB

Para acesso ao banco de dados DuckDB, é necessário referenciar o seguinte path na configuração do banco de dados: `/duckdb/my_database.duckdb`

### Acesso ao Container

Para validar os paths, rodar o comando: `docker container exec -it metaduck bash`