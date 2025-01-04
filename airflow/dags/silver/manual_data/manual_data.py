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

def default_data_quality(conn_duckdb, file_path):
    conn_duckdb.execute(f"CREATE TABLE dataset AS SELECT * FROM '{file_path}'")
    result = conn_duckdb.sql("SELECT column_name FROM (DESCRIBE dataset)").fetchdf()
    column_names = result['column_name'].tolist()

    # Data Quality for Null Values
    for column in column_names:
        null_test_query = f"""
            SELECT 
                '{column}' AS column_name,
                COUNT(*) AS total_rows,
                SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS null_count,
                (SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS null_percentage
            FROM dataset
        """
        conn_duckdb.sql(null_test_query).show()
        # null_test_result['Test'] = 'Null Check'
        # quality_results = pd.concat([quality_results, null_test_result.rename(columns={'column_name': 'Column'})], ignore_index=True)

@dag(
    dag_id="manual_data_transform",
    schedule_interval=None,
    start_date=datetime(2024, 11, 10),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["MANUAL_DATA", "TRANSFORMATION", "SILVER"],
)

def manual_data_transform():

    dag_path = "/opt/airflow/dags/silver/manual_data"
    source_path = "s3://bronze/datadrop" # s3://bronze/datadrop/kaggle/database.csv
    destination_path = "s3://silver/datadrop"
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

            conn = duckdb.connect()
            _duckdb_config(conn)
            print(f'Creating in-memory table {destination_table_name}...')
            conn.sql(query)

            print(f'Save table {destination_table_name} on storage...')
            conn.sql(f"COPY (SELECT * FROM {destination_table_name}) TO '{destination_path}/{destination_table_name}.parquet' (FORMAT 'parquet');")

            print(f'Closing connection')
            conn.close()

        @task(task_id=f"data_quality_tests_{destination_table_name}")
        def data_quality_tests(destination_table_name):
            s3_path = f"{destination_path}/{destination_table_name}.parquet"
            conn = duckdb.connect()
            _duckdb_config(conn)
            default_data_quality(conn, s3_path)
            conn.close()

        transform_task = transform_data(source_table_name, source_file_name, destination_table_name, query)
        quality_task = data_quality_tests(destination_table_name)
        transform_task >> quality_task
            
manual_data_transform()