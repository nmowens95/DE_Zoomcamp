from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ny_taxi_pipeline',
    default_args=default_args,
    description='ETL pipeline for NY Taxi data',
    schedule_interval='@monthly',
    catchup=False
) as dag:
    
    etl_postgres_task = BashOperator(
        task_id='etl_postgres',
        bash_command='python3 /opt/airflow/postgres_pipeline/etl_postgres.py'
    )

    postgres_clean_task = BashOperator(
        task_id='clean_data',
        bash_command='python3 /opt/airflow/postgres_pipeline/data_cleaning.py'
    )

    upload_to_gcs_task = BashOperator(
        task_id='upload_to_gcs',
        bash_command='python3 gcs_pipeline/load_to_gcs.py'
    )

# Define DAG task order
etl_postgres_task >> postgres_clean_task >> upload_to_gcs_task
# postgres_clean_task

    # airflow tasks test ny_taxi_pipeline etl_postgres 2024-02-05
