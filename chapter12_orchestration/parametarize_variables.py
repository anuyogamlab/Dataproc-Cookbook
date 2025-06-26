from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('use_composer_variable',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    def print_variable():
        sample_value = Variable.get('environment')
        print(f"The value of 'environment' is: {sample_value}")

    print_var_task = PythonOperator(
        task_id='print_composer_variable',
        python_callable=print_variable
    )

    print_var_task

