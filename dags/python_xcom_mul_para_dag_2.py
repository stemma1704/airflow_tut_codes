from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

default_args={
    'owner':'Stemy',
    'retires':5,
    'retry_delay':timedelta(minutes=5)
}
def greet(ti):
    first_name=ti.xcom_pull(task_ids='push_para',key='first_name')
    last_name=ti.xcom_pull(task_ids='push_para',key='last_name')
    age=ti.xcom_pull(task_ids='push_para',key='age')
    print(f"My name is {first_name} {last_name} and my age is {age}")
    
def push_para(ti):
    ti.xcom_push(key='first_name',value='Ketan')
    ti.xcom_push(key='last_name',value='Hasija')
    ti.xcom_push(key='age',value=30)
    
    
with DAG(
    dag_id='python_xcom_mul_para_dag_2',
    default_args=default_args,
    description='this is a python dag to accept all parameters using xcoms',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily'
    ) as dag:
    
    task1=PythonOperator(
        task_id='push_para',
        python_callable=push_para    
    )
    
    task2=PythonOperator(
        task_id='greet',
        python_callable=greet
    )
    
    task1>>task2
    #[task1,task2]>>task3 it means that task 1 & 2 will run parallely and then task 3
    
    