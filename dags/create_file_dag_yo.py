from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def create_hello_file():
    with open("/path/to/output/hello.txt", "w") as file:
        file.write("hello")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'create_hello_file_dag',
    default_args=default_args,
    schedule_interval=None,  # Run the DAG manually or on demand
    catchup=False,  # Don't backfill past runs
)

create_file_task = PythonOperator(
    task_id='create_file_task',
    python_callable=create_hello_file,
    dag=dag,
)

create_file_task