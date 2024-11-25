from datetime import datetime
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configuração para o MinIO (ou AWS S3)
MINIO_BUCKET = "bronze-data"
API_KEY = "sua_chave_api_aqui"  # Substitua pela sua chave da API API-Futebol

@dag(
    schedule_interval="0 0 * * *",  # Executa diariamente à meia-noite
    start_date=datetime(2024, 11, 26),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["API_FUTEBOL", "EXTRACTION", "BRONZE"],
)
def fetch_futebol_data():

    @task
    def fetch_campeonatos():
        """
        Fetch all available championships from the API.
        """
        url = "https://api.api-futebol.com.br/v1/campeonatos"
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        campeonatos = response.json()
        
        # Save data to S3
        s3 = S3Hook(aws_conn_id="minio_default")
        file_name = "campeonatos.json"
        s3.load_string(
            bucket_name=MINIO_BUCKET,
            key=file_name,
            string_data=json.dumps(campeonatos),
            replace=True,
        )
        return campeonatos

    @task
    def fetch_partidas(campeonatos):
        """
        Fetch all matches for each championship.
        """
        headers = {"Authorization": f"Bearer {API_KEY}"}
        partidas_data = {}
        for campeonato in campeonatos:
            campeonato_id = campeonato["campeonato_id"]
            url = f"https://api.api-futebol.com.br/v1/campeonatos/{campeonato_id}/partidas"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            partidas_data[campeonato_id] = response.json()

        # Save data to S3
        s3 = S3Hook(aws_conn_id="minio_default")
        file_name = "partidas.json"
        s3.load_string(
            bucket_name=MINIO_BUCKET,
            key=file_name,
            string_data=json.dumps(partidas_data),
            replace=True,
        )
        return partidas_data

    @task
    def fetch_jogadores(partidas):
        """
        Fetch player data for teams in Série A and Série B.
        """
        headers = {"Authorization": f"Bearer {API_KEY}"}
        jogadores_data = {}

        for campeonato_id, partidas_list in partidas.items():
            for partida in partidas_list:
                for time_id in [partida["time_mandante_id"], partida["time_visitante_id"]]:
                    url = f"https://api.api-futebol.com.br/v1/times/{time_id}"
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    jogadores_data[time_id] = response.json()

        # Save data to S3
        s3 = S3Hook(aws_conn_id="minio_default")
        file_name = "jogadores.json"
        s3.load_string(
            bucket_name=MINIO_BUCKET,
            key=file_name,
            string_data=json.dumps(jogadores_data),
            replace=True,
        )
        return jogadores_data

    campeonatos = fetch_campeonatos()
    partidas = fetch_partidas(campeonatos)
    fetch_jogadores(partidas)

fetch_futebol_data = fetch_futebol_data()