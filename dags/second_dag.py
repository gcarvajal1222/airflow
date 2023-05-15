from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

def say_yeah():
    print("yeah yeah yheah")

with DAG(
    dag_id='second_example_python_dag', #it needs to have the word dag
    start_date=datetime(2023, 5, 14),
    schedule_interval="@daily" #runs every minute
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    print_yeah = PythonOperator(
        task_id='print_yeah_python',
        python_callable=say_yeah
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> print_yeah
print_yeah >> end_task