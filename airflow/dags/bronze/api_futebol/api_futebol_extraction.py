from datetime import datetime
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import os

championship = [10]

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
    def fetch_match_index(championship):
        """
        Fetch all matches for each championship.
        """
        api_key = _get_credentials()
        headers = {"Authorization": f"Bearer {api_key}"}
        for id_championship in championship:
            url = f"https://api.api-futebol.com.br/v1/campeonatos/{id_championship}/partidas"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        
    @task
    def load_index_to_s3(data):
        """
        Load data into S3 or Minio
        """
        MINIO_BUCKET = "bronze"

        # Save data to S3
        s3 = S3Hook(aws_conn_id="minio_default")
        file_name = "api_futebol/campeonato_brasileiro/matches.json"
        s3.load_string(
            bucket_name=MINIO_BUCKET,
            key=file_name,
            string_data=json.dumps(data),
            replace=True,
        )

    @task
    def fetch_match_data(matches_index, round = None):
        base_url = 'https://api.api-futebol.com.br'
        championship_name = matches_index['campeonato']['slug']
        championship_name = championship_name.replace('-', '_')
        path = '/Users/arthurgonzaga/DataKick/notebooks/temp'
        api_key = _get_credentials()
        headers = {"Authorization": f"Bearer {api_key}"}
        s3 = S3Hook(aws_conn_id="minio_default")
        MINIO_BUCKET = "bronze"

        if round == None:
            for round_champ in matches_index['partidas']['fase-unica']:
                round = (round_champ.replace('-', '_'))
                for match in matches_index['partidas']['fase-unica'][round_champ]:
                    
                    id_match = match['partida_id']
                    endpoint = match['_link']
                    
                    url = base_url + endpoint
                    data = requests.get(url, headers=headers).json()
                    
                    path = f"api_futebol/{championship_name}/{round}/{id_match}.json"
                    
                    s3.load_string(
                        bucket_name=MINIO_BUCKET,
                        key=path,
                        string_data=json.dumps(data),
                        replace=True,
                    )
                   
        else:
            for match in matches_index['partidas']['fase-unica'][round]:
                    
                id_match = match['partida_id']
                endpoint = match['_link']
                
                url = base_url + endpoint
                data = requests.get(url, headers=headers).json()
                
                round = (round.replace('-', '_'))
                path = f"api_futebol/{championship_name}/{round}/{id_match}.json"
                
                s3.load_string(
                    bucket_name=MINIO_BUCKET,
                    key=path,
                    string_data=json.dumps(data),
                    replace=True,
                )

    
    response = fetch_match_index(championship)
    load_index_to_s3(response)
    fetch_match_data(response)

fetch_api_futebol_data()