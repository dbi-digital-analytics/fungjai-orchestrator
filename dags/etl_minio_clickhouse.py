from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from minio import Minio
from io import BytesIO
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="A_etl_minio_clickhouse",
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=["A_upload_file_to_minio"],
    description='DAG to upload a file to MinIO',
    max_active_runs=1,
):
    


    start = EmptyOperator(task_id="start")

    @task
    def download_file_from_minio():
        host = Variable.get("host")
        access_key = Variable.get("access_key")
        secret_key = Variable.get("secret_key")
        bucket = Variable.get("bucket")

        minioClient = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
        key = "data.csv"


        obj = minioClient.get_object(
                bucket,
                key,
        )
        df = pd.read_csv(obj, index_col=False)
        print("type", type(df))

    #     return obj

    # @task
    # def get_data_from_clickhouse(df):
        import clickhouse_connect

        client = clickhouse_connect.get_client(host='fpbcsf0ohz.us-east-2.aws.clickhouse.cloud', port=8443, username='default', password='qKuoSgpOOEVn')
        client.insert_df("test1", df)
        query_result = client.query('SELECT * FROM test1')
        print (query_result.result_set)


    end = EmptyOperator(task_id="end")

    download_file_from_minio_task = download_file_from_minio()
    # get_data_from_clickhouse_task = get_data_from_clickhouse(download_file_from_minio_task)

    start  >> download_file_from_minio_task >> end