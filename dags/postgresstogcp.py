import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='dag_exercise-postgres',
    default_args=args,
    schedule_interval='@daily',
) as dag:

    storeinbucket = PostgresToGoogleCloudStorageOperator(
        postgres_conn_id='exercise-postgres',
        sql= 'select transfer_date from land_registry_price_paid_uk limit 10',
        bucket= 'example_postgresstogcp',
        filename= 'data.json',
        schema_filename = 'schema.json',
        task_id= 'storeinbucket'
    )

    storagetoBQtable = GoogleCloudStorageToBigQueryOperator(
        bucket= 'example_postgresstogcp',
        source_objects = ['data.json'],
        schema_object= 'schema.json',
        destination_project_dataset_table= 'airflowbolcomdec-e4e4712278627.datafrompostgres.tabletest',
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition='WRITE_TRUNCATE',
        time_partitioning = 'transfer_date',
        task_id= 'storagetoBQtable'
    )

    storeinbucket >> storagetoBQtable
