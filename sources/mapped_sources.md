# Origens dos Dados

Temos algumas fontes de dados que são utilizadas para a construção da nossa plataforma de dados. A seguir, listamos as principais fontes de dados que utilizamos:

- [API Futebol](https://www.api-futebol.com/):
    - A API Futebol é uma API que fornece dados sobre campeonatos, times, jogadores, partidas e estatísticas de futebol. Utilizamos essa API para obter informações sobre os campeonatos e partidas de futebol. O foco é trazer dados do dia anterior para armazenar no nosso lakehouse.
    - A API Futebol fornece dados em formato JSON, que são consumidos por meio de requisições HTTP.
    - A API Futebol é uma API paga, mas oferece um plano gratuito que permite um número limitado de requisições por mês.
    - A documentação da API Futebol pode ser encontrada [aqui](https://www.api-futebol.com.br/documentacao).
    - Há ambiente de teste que gera dados fictícios

- [Football Data](https://www.football-data.org/):
    - API internacional que fornece dados sobre campeonatos, times, jogadores, partidas e estatísticas de futebol idem à API Futebol.
    - Toda a documetação pode ser encontrada [aqui](https://www.football-data.org/documentation/quickstart).
    - Não tem ambientes de testes.
    - Tem uma versão paga também, mas há a possibilidade de usar parcialmente de graça.

- [Kaggle](https://www.kaggle.com/):
    - Alguns datasets são disponibilizados diretamente no Kaggle:
        - [Player Stats 2024](https://www.kaggle.com/datasets/eduardopalmieri/brasileiro-player-stats-2024)

- [SportsMonk](https://my.sportmonks.com/dashboard):
    - API internacional que fornece dados sobre campeonatos, times, jogadores, partidas e estatísticas de futebol idem à API Futebol.
    - Toda a documetação pode ser encontrada [aqui](https://docs.sportmonks.com/football/welcome/getting-started).
    - Tem uma versão paga também, mas há a possibilidade de usar parcialmente de graça.