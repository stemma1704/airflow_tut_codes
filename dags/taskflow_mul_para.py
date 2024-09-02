from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'Stemy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='taskflow_mul_para',
     default_args=default_args,
     description='This is a Python DAG using TaskFlow API',
     start_date=datetime(2024, 7, 26),
     schedule_interval='@daily'
)
def hello_world(): 

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'RIYA',
            'last_name': 'NEGI'
        }

    @task()
    def get_age():
        return 25

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} and I'm {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag = hello_world()
