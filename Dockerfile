FROM apache/spark-py:v3.1.3

ARG SDK_VERSION="1.11.563"
ARG HADOOP_AWS_VERSION="3.2.0"
ARG BASE_URL="https://repo1.maven.org/maven2"
ARG WHEEL_VERSION="0.1.0-py3-none-any"

USER 0

RUN apt-get update -y &&\
    apt-get install curl python3-venv -y &&\
    curl "${BASE_URL}/com/amazonaws/aws-java-sdk-bundle/${SDK_VERSION}/aws-java-sdk-bundle-${SDK_VERSION}.jar" -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${SDK_VERSION}.jar" &&\
    curl "${BASE_URL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" &&\
    apt-get purge curl -y &&\
    apt-get autoremove -y &&\
    rm -rf /root/.cache /var/cache/apt/*

RUN mkdir ${SPARK_HOME}/venv && chown 185 ${SPARK_HOME}/venv
USER 185

ENV PATH=$SPARK_HOME/venv/bin:$PATH
RUN python3 -m venv ${SPARK_HOME}/venv
RUN pip install --no-cache-dir wheel pyarrow requests \
    "https://github.com/news-events-miner/event-detection/releases/download/alpha/event_detection-${WHEEL_VERSION}.whl" --no-cache-dir

COPY ./spark_job /opt/spark/python/custom_jobs/event_extractor
