from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.decorators import dag,task

default_args={
    'owner':'Stemy',
    'retires':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id='taskflow_single_para',
    default_args=default_args,
    description='this is a python dag using taskflow api',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily'
) 
def hello_world(): #main function inside this we will have our other function before which the decorator will be called.
    
    @task()
    def get_name():
        return "Riya"
    
    @task()
    def get_age():
        return 25
    
    @task()
    def greet(name,age):
        print(f"Hello World! My name is {name} and I'm {age} years old.")
        
    name=get_name()
    age=get_age()
    greet(name=name,age=age)
    
greet_dag=hello_world()