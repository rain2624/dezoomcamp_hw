import os
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_function import ingest_callable
import psycopg2

AIRFLOW_HOME= os.environ.get("AIRFLOW_HOME","/opt/airflow")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD') 
PG_PORT = os.getenv('PG_PORT') 
PG_DATABASE = os.getenv('PG_DATABASE') 


local_workflow = DAG(
    "data_ingestion_gcs_dag_v02",
    schedule_interval='0 18 2 * *',
    start_date=datetime(2019,1,1),
    end_date=datetime(2019,12,31),
    catchup=True,
    max_active_runs=3  # limits the number of active runs at a time
)

url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
url_template = url_prefix + 'yellow_tripdata_{{ execution_date .strftime(\'%Y-%m\') }}.parquet'
output_file = AIRFLOW_HOME + '/output_{{ execution_date .strftime(\'%Y-%m\') }}.parquet'
table_name = 'yellow_tripdata_{{ execution_date .strftime(\'%Y_%m\') }}'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget_task',
        bash_command= f'curl -sSLf {url_template} > {output_file}'
    )

    ingest_task = PythonOperator(
        task_id='ingest_task',
        python_callable =ingest_callable,
        op_kwargs=dict(
            user = PG_USER,
            password = PG_PASSWORD,
            host = PG_HOST,
            port = PG_PORT,
            db = PG_DATABASE,
            table_name= table_name,
            parquet_file=output_file),
    )

    cleanup_task = BashOperator(
    task_id='cleanup_task',
    bash_command=f'rm {output_file}'
)

    wget_task >> ingest_task >> cleanup_task