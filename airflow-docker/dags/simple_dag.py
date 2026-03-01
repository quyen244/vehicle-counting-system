import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print("Xin chào từ Airflow Task!")
    return "Hello World"

with DAG(
    dag_id="airflow_tutorial_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "tutorial"],
) as dag:

    task_bash = BashOperator(
        task_id="run_bash_command",
        bash_command="echo 'Bắt đầu quy trình Airflow'",
    )

    task_python = PythonOperator(
        task_id="run_python_function",
        python_callable=print_hello,
    )

    task_final = BashOperator(
        task_id="finish_message",
        bash_command="echo 'Quy trình đã hoàn thành!'",
    )

    task_bash >> task_python >> task_final
