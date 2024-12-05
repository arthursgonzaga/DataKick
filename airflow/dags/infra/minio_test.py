from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    start_date=datetime(2024, 11, 10),
    catchup=False,
    tags=["INFRA"]
)

def teste_minio():
    @task
    def test_minio_connection():
        s3 = S3Hook(aws_conn_id="minio_default")
        print(s3.list_keys(bucket_name="bronze"))

    test_minio_connection()

teste_minio()