import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator.BigQueryOperator import BigQueryOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='BQ_scheduler_dag',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    scheduled_count = BigQueryOperator(
        task_id='scheduled_count',
        sql ='select count(*) from airflowbolcomdec-e4e4712278627.datafrompostgres.tabletest',
        destination_project_dataset_table='airflowbolcomdec-e4e4712278627.datafrompostgres.counts',
        write_disposition = 'WRITE_APPEND'
    )

    scheduled_count
