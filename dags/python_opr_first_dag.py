from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

default_args={
    'owner':'Stemy',
    'retires':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(name,age):
    print(f"Hello World!My name is {name} and I'm {age} years old.")

with DAG(
    default_args=default_args,
    dag_id='first_python_dag',
    description='writing my first python operator dag',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily'
    ) as dag:
    
    task1=PythonOperator(
        task_id='greet_python_opr',
        python_callable=greet,
        op_kwargs={'name':'Stemy','age':26}
    )
    