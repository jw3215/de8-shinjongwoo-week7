"""Q7 — 백필(catchup).

start_date 를 작업일 기준 7일 전 날짜로 고정하고 catchup=True 로 두면,
DAG 를 켜는 순간 start_date 부터 오늘까지의 매일 실행이 scheduled 로 한꺼번에 채워집니다.
각 실행은 자신의 logical date(ds) 이름의 파일을 컨테이너 안에 씁니다.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

OUT_DIR = "/tmp/backfill_demo"

with DAG(
    dag_id="backfill_demo_shinjongwoo",
    schedule="@daily",  # 매일 1회
    start_date=pendulum.datetime(2026, 11, 5, tz="UTC"),  # 작업일(2026-11-12) 기준 7일 전, 고정 날짜
    catchup=True,  # 과거 구간 실행을 자동으로 채움 (Airflow 3 기본값은 False)
    max_active_runs=2,
    tags=["week7", "q7"],
):
    # {{ ds }} 는 실행마다 그 실행의 logical date(YYYY-MM-DD)로 렌더링됩니다.
    write_daily_file = BashOperator(
        task_id="write_daily_file",
        bash_command=(
            "mkdir -p " + OUT_DIR + " && "
            "echo 'run for {{ ds }} ({{ run_id }})' > " + OUT_DIR + "/daily_{{ ds }}.txt && "
            "cat " + OUT_DIR + "/daily_{{ ds }}.txt"
        ),
    )
    verify_file = BashOperator(
        task_id="verify_file",
        bash_command="test -s " + OUT_DIR + "/daily_{{ ds }}.txt && ls -l " + OUT_DIR,
    )

    write_daily_file >> verify_file
