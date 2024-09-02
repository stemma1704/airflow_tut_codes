from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'Stemy',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag',
    description='This is our first dag creation',
    default_args=default_args,
    start_date=datetime(2024,7,25,8,0,0),
    schedule_interval='@daily'
    
    ) as dag:
    
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world!"
    )