"""Q9 — netflix_titles.csv 를 type × 장르별 작품 수로 집계해 parquet(snappy)로 저장.

  spark-submit transform.py --input <csv> --output <dir> [--min-year 2015]

- release_year 가 기준 연도 이상인 행만 남깁니다. 기준 연도는 인자로 주입하고 기본값은 2015 입니다.
- listed_in 은 쉼표로 구분된 장르 목록이므로 장르 하나당 한 행이 되도록 펼치고 앞뒤 공백을 제거합니다.
- 결과 행 수를 출력합니다.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--min-year", type=int, default=2015)
args = parser.parse_args()

spark = SparkSession.builder.appName("netflix_type_genre").getOrCreate()

# description 등에 따옴표로 감싼 줄바꿈이 있어 multiLine 으로 읽어야 레코드가 깨지지 않습니다.
df = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(args.input)
)
print(f"input rows = {df.count()}")

result = (
    df.where(F.col("release_year").cast("int") >= args.min_year)
    .withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
    .withColumn("genre", F.trim(F.col("genre")))
    .where(F.col("genre") != "")
    .groupBy("type", "genre")
    .agg(F.count("*").alias("title_count"))
    .orderBy(F.col("type"), F.col("title_count").desc(), F.col("genre"))
)

result.show(100, truncate=False)
row_count = result.count()
print(f"min_year = {args.min_year}")
print(f"aggregated rows = {row_count}")

result.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(args.output)
print(f"written parquet(snappy) -> {args.output}")

spark.stop()
