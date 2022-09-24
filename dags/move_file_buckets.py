from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from os import getenv

BUCKET_NAME_RECEPTION   = getenv('BUCKET_NAME_RECEPTION', 'reception-bucket')
BUCKET_NAME_DESTINATION = getenv('BUCKET_NAME_DESTINATION', 'destination-zone')

default_args = {
    'owner': 'Ayrton Cossuol',
    'depends_on_past': False, #depende de uma execucao anterior
    'email': ['ayrton.cossuol@live.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1 #quantidade de tentativas
}

with DAG(
    dag_id = 'move_file_buckets',
    tags = ['development', 'cloud storage', 'movimentation'],
    default_args = default_args,
    schedule_interval = None,
    catchup = False
) as dag:

    move_files = GCSToGCSOperator(
        task_id = "move_files",
        source_bucket = BUCKET_NAME_RECEPTION,
        source_object = 'NIFTY BANK*',
        destination_bucket = BUCKET_NAME_DESTINATION,
        move_object = True,
    )

    list_files_move = GCSListObjectsOperator(
        task_id = 'list_files_move',
        bucket = BUCKET_NAME_DESTINATION,
        delimiter = '.csv',
    )

    initial_task = DummyOperator(task_id = 'initial_task')
    finish_task = DummyOperator(task_id = 'finish_task')

    initial_task >> move_files >> list_files_move >> finish_task