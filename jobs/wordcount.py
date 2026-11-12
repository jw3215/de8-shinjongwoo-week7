"""Q4 — Spark Standalone 클러스터에서 실행하는 단어 빈도 집계.

처리 흐름: 텍스트 파일 읽기 → 공백 기준 단어 분리 → 단어별 개수 집계 → 빈도 내림차순 상위 20개 show()
분리 규칙은 공백뿐입니다. 소문자 변환·구두점 제거를 하지 않으므로 'Lorem' 과 'lorem', 'amet' 과 'amet,' 은 다른 단어입니다.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/wordcount.txt"

spark = SparkSession.builder.appName("WordCount_shinjongwoo").getOrCreate()

lines = spark.read.text(input_path)

# 공백(스페이스·탭·줄바꿈) 기준으로만 자르고, 연속 공백이 만든 빈 문자열은 버립니다.
words = lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word")).where(F.col("word") != "")

counts = words.groupBy("word").count()

total_words = words.count()
distinct_words = counts.count()
print(f"total_words={total_words}  distinct_words={distinct_words}")

# 같은 빈도끼리는 단어 사전순으로 고정해 실행마다 순서가 흔들리지 않게 합니다.
counts.orderBy(F.col("count").desc(), F.col("word").asc()).show(20, truncate=False)

spark.stop()
