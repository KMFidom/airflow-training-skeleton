import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='dag_exercise-postgres',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    task1 = PostgresToGoogleCloudStorageOperator(postgres_conn_id='exercise_postgres',
                                                  sql= 'select transfer_date from land_registry_price_paid_uk limit 10',
                                                  bucket= 'example_postgresstogcp',
                                                  filename= 'test',
                                                task_id= 'storeinbucket'
                                                  )
