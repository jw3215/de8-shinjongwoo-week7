"""Q9 — S3 → Spark → S3 를 세 개의 task 로 잇는 통합 파이프라인.

  download_csv  : boto3 로 s3://{버킷}/bronze/netflix_titles.csv 를 컨테이너 안으로 내려받기
  transform     : spark-submit 으로 jobs/transform.py 실행 (type × 장르 집계 → parquet/snappy)
  upload_silver : boto3 로 결과를 s3://{버킷}/silver/{실행 날짜}/ 에 올리고 목록·개수 출력

버킷 이름은 환경변수 S3_BUCKET(.env → 컨테이너)으로 받고, 자격증명은 코드·yaml·.env 어디에도 없습니다.
컨테이너에 읽기 전용으로 마운트한 ~/.aws 의 프로필(AWS_PROFILE)을 boto3 기본 체인이 읽습니다.
"""
import os
from pathlib import Path

import boto3
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# Airflow 3 에서는 수동 트리거 실행에 logical date 가 없어 {{ ds }} 가 비어 있습니다.
# 그래서 모든 실행에 항상 있는 run_after(실행 기준 시각)로 날짜·작업 폴더를 정합니다.
RUN_DATE = "{{ dag_run.run_after.strftime('%Y-%m-%d') }}"
WORK_DIR = "/tmp/weekly_pipeline/{{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}"  # 실행별 작업 폴더 (컨테이너 안)
SRC_KEY = "bronze/netflix_titles.csv"
JOB = "/opt/airflow/dags/jobs/transform.py"


def _bucket() -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET 환경변수가 없습니다 (.env 확인)")
    return bucket


def download_csv(work_dir: str):
    bucket = _bucket()
    local = Path(work_dir) / "netflix_titles.csv"
    local.parent.mkdir(parents=True, exist_ok=True)
    boto3.client("s3").download_file(bucket, SRC_KEY, str(local))
    print(f"downloaded s3://{bucket}/{SRC_KEY} -> {local} ({local.stat().st_size:,} bytes)")


def upload_silver(work_dir: str, run_date: str):
    bucket = _bucket()
    s3 = boto3.client("s3")
    out_dir = Path(work_dir) / "output"
    prefix = f"silver/{run_date}/"
    for f in sorted(out_dir.glob("*.parquet")):
        s3.upload_file(str(f), bucket, prefix + f.name)
        print(f"uploaded {f.name} -> s3://{bucket}/{prefix}{f.name}")

    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    print(f"s3://{bucket}/{prefix} objects:")
    for o in objs:
        print(f"  {o['Key']}  {o['Size']:,} bytes")
    print(f"object count = {len(objs)}")


with DAG(
    dag_id="weekly_pipeline_shinjongwoo",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["week7", "q9"],
):
    t_download = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
        op_kwargs={"work_dir": WORK_DIR},
    )
    t_transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"spark-submit --master 'local[*]' {JOB} "
            f"--input {WORK_DIR}/netflix_titles.csv "
            f"--output {WORK_DIR}/output "
            "--min-year {{ params.min_year }}"
        ),
        params={"min_year": 2015},
    )
    t_upload = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
        op_kwargs={"work_dir": WORK_DIR, "run_date": RUN_DATE},
    )

    t_download >> t_transform >> t_upload
