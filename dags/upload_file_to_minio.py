from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.empty import EmptyOperator
from minio import Minio
from io import BytesIO
import pandas as pd

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 15),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="A_upload_file_to_minio",
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=["A_upload_file_to_minio"],
    description="DAG to upload a file to MinIO",
    max_active_runs=1,
):
    # host = Variable.get("host")
    # access_key = Variable.get("access_key")
    # secret_key = Variable.get("secret_key")
    # minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    # bucket = Variable.get("bucket")
    key = "data.csv"

    start = EmptyOperator(task_id="start")

    @task
    def upload_file():
        df = pd.DataFrame(
            {
                "name": ["Raphael", "Donatello"],
                "mask": ["red", "purple"],
                "weapon": ["sai", "bo staff"],
            }
        )
        csv = df.to_csv(index=False).encode("utf-8")

        minioClient.put_object(bucket, key, data=BytesIO(csv), length=len(csv))

        print("put object Done!")

    @task
    def download_file():
        obj = minioClient.get_object(
            bucket,
            key,
        )
        df = pd.read_csv(obj)
        print(df)

    end = EmptyOperator(task_id="end")

    upload_file_task = upload_file()
    download_file_task = download_file()

    start >> upload_file_task >> download_file_task >> end
