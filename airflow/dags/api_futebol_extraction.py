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
            return response.json()
        
    @task
    def load_json(data):
        """
        Load data into S3 or Minio
        """
        MINIO_BUCKET = "bronze"

        # Save data to S3
        s3 = S3Hook(aws_conn_id="minio_default")
        file_name = "api_futebol/matches.json"
        s3.load_string(
            bucket_name=MINIO_BUCKET,
            key=file_name,
            string_data=json.dumps(data),
            replace=True,
        )
    
    response = fetch_partidas(campeonatos)
    load_json(response)


fetch_api_futebol_data()