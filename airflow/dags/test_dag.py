from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
  print("Hello, World!")

with DAG('helo_world', start_date=datetime(2025, 6, 1), schedule_interval='@daily') as dag:
  task = PythonOperator(task_id='hello_task', python_callable=hello_world)
  task