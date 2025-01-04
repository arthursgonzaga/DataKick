from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.utils.email import send_email
import duckdb
import json
import yaml
import os
import pandas as pd

def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'arthursgonzaga@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

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
    dag_id="golden_sets",
    schedule_interval=None,
    start_date=datetime(2024, 11, 10),
    catchup=False,
    tags=["TRANSFORMATION", "GOLD"],
)

def data_transform():

    dag_path = "/opt/airflow/dags/gold/golden_sets"
    source_path = "s3://silver/"
    destination_path = "s3://gold/"
    config = read_table_config(f"{dag_path}/config.yaml")
    tables = config.get('tables', {})

    for key, value in tables.items():
        destination_table_name = key
        source_table_name = value.get('table_name')
        source_file_name = value.get('file_name')      
        query = load_query(dag_path, destination_table_name)

        @task(
            task_id=f"transform_{destination_table_name}",      
            on_failure_callback = failure_email
        )
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