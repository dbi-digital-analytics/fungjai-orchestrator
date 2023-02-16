from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

@task
def get_data_from_clickhouse():
    import clickhouse_connect
    client = clickhouse_connect.get_client(host='fpbcsf0ohz.us-east-2.aws.clickhouse.cloud', port=8443, username='default', password='qKuoSgpOOEVn')
    query_result = client.query('SELECT * FROM test1')
    print (query_result.result_set)


with DAG(
        dag_id='A_get_data_from_clickhouse',
        start_date=days_ago(2),
) as dag:

    get_data_from_clickhouse_task = get_data_from_clickhouse()
