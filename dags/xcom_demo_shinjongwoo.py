"""Q6 — XCom 으로 값 전달 + 재시도.

count_lines 가 return 한 값은 XCom 의 return_value 키로 메타DB 에 저장되고,
use_value 는 ti.xcom_pull 로 그 값을 꺼내 씁니다.
count_lines 는 첫 시도에서만 일부러 실패해, retries 설정으로 두 번째 시도에 성공합니다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SAMPLE_LINES = [
    "2026-11-12 INFO  pipeline started",
    "2026-11-12 INFO  download done",
    "2026-11-12 ERROR spark job failed",
    "2026-11-12 INFO  retry scheduled",
    "2026-11-12 INFO  spark job done",
    "2026-11-12 INFO  upload done",
]


def count_lines(ti):
    # 첫 시도에서만 실패시키고, 재시도(try_number >= 2)에서는 정상 처리합니다.
    print(f"try_number = {ti.try_number}")
    if ti.try_number == 1:
        raise RuntimeError("의도한 실패: 첫 시도는 실패시켜 재시도 동작을 확인합니다.")
    n = len(SAMPLE_LINES)
    print(f"line count = {n}")
    return n  # → XCom(key='return_value') 로 저장


def use_value(ti):
    n = ti.xcom_pull(task_ids="count_lines", key="return_value")
    print(f"received from XCom: {n}")
    print(f"doubled = {n * 2}")


with DAG(
    dag_id="xcom_demo_shinjongwoo",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["week7", "q6"],
):
    t1 = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    t2 = PythonOperator(task_id="use_value", python_callable=use_value)

    t1 >> t2
