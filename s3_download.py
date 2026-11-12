"""Q8 — boto3 로 S3 의 bronze/ 를 읽어오는 스크립트.

한 번의 실행에서 순서대로:
  1. s3://{버킷}/bronze/ 아래 객체 목록과 각 객체의 크기 출력
  2. bronze/netflix_titles.csv 를 project/data/ 아래로 다운로드
  3. 내려받은 파일의 행 수(CSV 레코드 수, 헤더 제외) 출력

버킷 이름은 코드에 적지 않고 실행 인자 또는 환경변수 S3_BUCKET 으로 받습니다.
자격증명은 코드에 없습니다 — boto3 기본 체인(AWS_PROFILE 등)을 그대로 씁니다.

사용법:
  AWS_PROFILE=metacode python s3_download.py de-8-shinjongwoo
  S3_BUCKET=de-8-shinjongwoo AWS_PROFILE=metacode python s3_download.py
"""
import csv
import os
import sys
from pathlib import Path

import boto3

PREFIX = "bronze/"
KEY = "bronze/netflix_titles.csv"
DATA_DIR = Path(__file__).resolve().parent / "data"


def main() -> None:
    bucket = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("S3_BUCKET")
    if not bucket:
        sys.exit("버킷 이름을 실행 인자 또는 S3_BUCKET 환경변수로 넘겨 주세요.")

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))

    # 1) 목록 · 크기
    print(f"[1] list     s3://{bucket}/{PREFIX}")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            print(f"    {obj['Key']:<45} {obj['Size']:>12,} bytes")

    # 2) 다운로드
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    local_path = DATA_DIR / Path(KEY).name
    print(f"[2] download s3://{bucket}/{KEY} -> data/{local_path.name}")
    s3.download_file(bucket, KEY, str(local_path))
    print(f"    downloaded ({local_path.stat().st_size:,} bytes)")

    # 3) 행 수 — 줄 수가 아니라 CSV 레코드 수.
    #    csv 모듈이 따옴표 안의 줄바꿈을 한 필드로 읽으므로, 여러 줄짜리 description 도 한 행으로 셉니다.
    with open(local_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # 헤더 제외
        rows = sum(1 for _ in reader)
    print(f"[3] rows     {rows:,} (CSV records, header excluded)")


if __name__ == "__main__":
    main()
