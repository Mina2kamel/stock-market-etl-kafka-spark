FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# # Download JARs needed for S3/MinIO/Kafka
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.4.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.4/spark-sql-kafka-0-10_2.12-3.4.4.jar

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3 && \
    pip install -r requirements.txt
