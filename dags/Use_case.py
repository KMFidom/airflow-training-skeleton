import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from operators.http_to_gcs_operator import HttpToGcsOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='use_case',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    connect_api = HttpToGcsOperator(
        task_id='connect_api',
        http_conn_id = 'https://api.exchangeratesapi.io/',
        endpoint = 'history?start_at=2018-01-01&end_at=2018-01-02&symbols=EUR&base=GBP',
        gcs_bucket = 'use_case_airflow'
    )

    connect_api
