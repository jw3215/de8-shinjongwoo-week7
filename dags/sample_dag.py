"""Q3 — 시작 → 파이썬 작업 2개 → 종료, 4개 task 를 순차 연결한 기본 DAG."""
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello() -> str:
    return "Hello Airflow"


def say_goodbye() -> str:
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    schedule="@daily",  # 매일 1회
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # 과거 구간은 자동 실행하지 않음
    tags=["week7", "q3"],
):
    start = EmptyOperator(task_id="start")
    hello_task = PythonOperator(task_id="hello_task", python_callable=say_hello)
    goodbye_task = PythonOperator(task_id="goodbye_task", python_callable=say_goodbye)
    end = EmptyOperator(task_id="end")

    start >> hello_task >> goodbye_task >> end
