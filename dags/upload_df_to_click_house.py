from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

# @task
# def get_data_from_clickhouse():
#     import clickhouse_connect
#     client = clickhouse_connect.get_client(host='fpbcsf0ohz.us-east-2.aws.clickhouse.cloud', port=8443, username='default', password='qKuoSgpOOEVn')
#     query_result = client.query('SELECT * FROM test1')
#     print (query_result.result_set)


with DAG(
    dag_id="A_get_data_from_clickhouse",
    start_date=days_ago(2),
) as dag:
    (
        ClickHouseOperator(
            task_id="update_income_aggregate",
            database="default",
            sql=(
                """
            SELECT * FROM system.clusters
        """,
                # result of the last query is pushed to XCom
            ),
            clickhouse_conn_id="clickhouse_test",
        )
        >> PythonOperator(
            task_id="print_month_income",
            provide_context=True,
            python_callable=lambda task_instance, **_:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids="update_income_aggregate")),
        )
    )
