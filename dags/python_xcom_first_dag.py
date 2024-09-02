from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

default_args={
    'owner':'Stemy',
    'retires':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(age,ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello World!My name is {name} and I'm {age} years old.")
    
def get_name():
    return 'Jerry'

with DAG(
    default_args=default_args,
    dag_id='python_xcom_first_dag',
    description='writing my first python operator dag with xcom',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily'
    ) as dag:
    
    task1=PythonOperator(  #now we pull the name 'Jerry' for parameter name
        task_id='greet_python_opr',
        python_callable=greet,
        op_kwargs={'age':26}
    )
    
    task2=PythonOperator(  #first we push the name Jerry into xcom
        task_id='get_name',
        python_callable=get_name
    )
    
    task2>>task1 #task2 gets executed first then task1 as there dependency.push then pull