# 데엔 8기 신종우 — 7주차 실습

- 이름: 신종우
- 기수: 데엔 8기

Docker Compose 로 Airflow · Spark 환경을 직접 구성하고, S3 와 연결한 파이프라인까지 이어 붙이는 7주차 과제 레포지토리입니다.

| 문항 | 내용 | 파일 |
|---|---|---|
| Q3 | 공식 compose 로 Airflow 환경 구축 · 기본 DAG | `docker-compose.yaml`, `dags/sample_dag.py` |
| Q4 | Spark Standalone 클러스터 + wordcount | `docker-compose.spark.yaml`, `jobs/wordcount.py` |
| Q5 | 커스텀 Airflow 이미지 (JDK · provider · pyspark) | `Dockerfile`, `requirements.txt` |
| Q6 | XCom 전달 · 재시도 | `dags/xcom_demo_shinjongwoo.py` |
| Q7 | 백필(catchup) | `dags/backfill_demo_shinjongwoo.py` |
| Q8 | boto3 로 S3 목록 · 다운로드 · 행 수 | `s3_download.py` |
| Q9 | S3 → Spark → S3 통합 파이프라인 | `dags/weekly_pipeline_shinjongwoo.py`, `dags/jobs/transform.py` |

## 실행
```bash
docker compose up airflow-init
docker compose up -d
# UI: http://localhost:8080  (airflow / airflow)
```
