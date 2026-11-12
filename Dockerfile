# Q5 — 공식 Airflow 이미지를 베이스로 JDK · provider · pyspark 를 더한 커스텀 이미지
FROM apache/airflow:3.4.0

# 1) JDK — apt 로 시스템 패키지를 설치하려면 root 로 전환해야 합니다.
#    베이스가 Debian 13(trixie)이라 default-jdk-headless 는 OpenJDK 21 입니다. (Spark 4.x 는 Java 17/21 지원)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk-headless procps \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 2) 파이썬 패키지 — pip 는 airflow 사용자로 설치합니다.
#    apache-airflow 를 같은 버전으로 함께 적어, provider 설치가 Airflow 본체를 업/다운그레이드하지 못하게 고정합니다.
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
