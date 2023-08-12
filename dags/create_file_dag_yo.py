from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def create_hello_file():
    print("whats up dude")
    import sys
    current_path = sys.path[0]
    print("Current working directory:", current_path)
    file_name = "this_is_the_file_to_see.txt"
    content = "This is the content of the file."

    with open(file_name, "w") as file:
        file.write(content)

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