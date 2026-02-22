FROM bitnami/spark:3.5.1

USER root

# ─── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ─── Hadoop AWS + AWS SDK JARs ────────────────────────────────────────────────
ENV SPARK_HOME=/opt/bitnami/spark
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
        -O "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        -O "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ─── Spark config for S3A + LocalStack ────────────────────────────────────────
COPY docker/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# ─── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/spark/work-dir

USER 1001
