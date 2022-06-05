FROM apache/spark-py:v3.2.1

ARG SDK_VERSION="1.12.233"
ARG HADOOP_AWS_VERSION="3.3.3"
ARG BASE_URL="https://repo1.maven.org/maven2"
ARG WHEEL_VERSION="0.1.0-py3-none-any"

USER 0

RUN apt-get update -y &&\
    apt-get install curl -y &&\
    pip install \
    "https://github.com/news-events-miner/event-detection/releases/download/alpha/event_detection-${WHEEL_VERSION}.whl" --no-cache-dir &&\
    rm -rf /root/.cache /var/cache/apt/*

RUN curl "${BASE_URL}/com/amazonaws/aws-java-sdk-bundle/${SDK_VERSION}/aws-java-sdk-bundle-${SDK_VERSION}.jar" -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${SDK_VERSION}.jar"
RUN curl "${BASE_URL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

COPY ./spark_job /opt/spark/python/custom_jobs/event_extractor
USER 185

