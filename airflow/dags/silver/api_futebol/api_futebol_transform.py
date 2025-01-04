from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import duckdb
import json
import yaml
import os
import pandas as pd

def _duckdb_config(conn_duckdb):
    connection = BaseHook.get_connection("minio_default")
    minio_config = json.loads(connection.extra)
    
    MINIO_ACCESS_KEY = minio_config.get('aws_access_key_id')
    MINIO_SECRET_KEY = minio_config.get('aws_secret_access_key')
    MINIO_ENDPOINT = minio_config.get('endpoint_url')
    MINIO_ENDPOINT = MINIO_ENDPOINT.removeprefix("http://")

    config_query = f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_endpoint='{MINIO_ENDPOINT}'; -- Endpoint do MinIO
        SET s3_url_style = 'path';
        SET s3_use_ssl=false;
        """
    
    try:
        print('Executing configuration setup:')
        conn_duckdb.execute(config_query)
    except:
        print('Problem with setup configuration in DuckDB')

def read_table_config(file_path='config.yaml'):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{file_path}' não encontrado.")
    except yaml.YAMLError as e:
        print(f"Erro ao processar o arquivo YAML: {e}")

def load_query(dag_path, table_name):
    query_path = os.path.join(f"{dag_path}/queries", f"{table_name}.sql")
    try:
        with open(query_path, 'r') as file:
            query = file.read()
        return query
    except FileNotFoundError:
        print(f"Erro: Arquivo de query '{query_path}' não encontrado.")
        return None

@dag(
    dag_id="api_futebol_transform",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 11, 10),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["API_FUTEBOL", "TRANSFORMATION", "SILVER"],
)

def data_transform():

    dag_path = "/opt/airflow/dags/silver/api_futebol"
    source_path = "s3://bronze/api_futebol"
    destination_path = "s3://silver/api_futebol"
    config = read_table_config(f"{dag_path}/config.yaml")
    tables = config.get('tables', {})

    for key, value in tables.items():
        destination_table_name = key
        source_table_name = value.get('table_name')
        source_file_name = value.get('file_name')      
        query = load_query(dag_path, destination_table_name)

        @task(task_id=f"transform_{destination_table_name}")
        def transform_data(source_table_name, source_file_name, destination_table_name, query):

            s3_path = f"{source_path}/{source_table_name}/{source_file_name}"
            query = query.format(s3_path = s3_path, destination_table_name = destination_table_name)

            print(query)

            conn = duckdb.connect()
            _duckdb_config(conn)
            print(f'Creating in-memory table {destination_table_name}...')
            conn.sql(query)

            print(f'Save table {destination_table_name} on storage...')
            conn.sql(f"COPY (SELECT * FROM {destination_table_name}) TO '{destination_path}/{destination_table_name}.parquet' (FORMAT 'parquet');")

            print(f'Closing connection')
            conn.close()

        transform_task = transform_data(source_table_name, source_file_name, destination_table_name, query)
        transform_task
            
data_transform()