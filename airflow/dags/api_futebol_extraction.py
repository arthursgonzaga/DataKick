from datetime import datetime
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

campeonatos = [10]

def _get_credentials():
    """
    Obtém a chave da API armazenada nas variáveis do Airflow.
    """
    api_key = Variable.get("KEY_API_FUTEBOL_DEV")
    return api_key

@dag(
    dag_id="api_futebol_extraction",
    schedule_interval="0 0 * * *",  # Executa diariamente à meia-noite
    start_date=datetime(2024, 11, 10),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["API_FUTEBOL", "EXTRACTION", "BRONZE"],
)

def fetch_api_futebol_data():
    @task
    def fetch_partidas(campeonatos):
        """
        Fetch all matches for each championship.
        """
        api_key = _get_credentials()
        headers = {"Authorization": f"Bearer {api_key}"}
        for campeonato_id in campeonatos:
            url = f"https://api.api-futebol.com.br/v1/campeonatos/{campeonato_id}/partidas"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            # print(response.json())
            return response.json()
    
    fetch_partidas(campeonatos)


fetch_api_futebol_data()