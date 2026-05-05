FROM apache/spark:3.4.2

USER root

COPY requirements.txt /
RUN pip install -r /requirements.txt

# Hudi 0.14.1 + AWS SDK for S3A + Kafka client for Spark Streaming
WORKDIR /opt/spark/jars
RUN curl -O https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.1/hudi-spark3.4-bundle_2.12-0.14.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.2/spark-sql-kafka-0-10_2.12-3.4.2.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.2/spark-token-provider-kafka-0-10_2.12-3.4.2.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY entrypoint.sh /opt/entrypoint.sh
COPY metrics.properties /opt/spark/conf/metrics.properties
RUN chmod +x /opt/entrypoint.sh

WORKDIR /opt/spark
USER 185

ENTRYPOINT ["/opt/entrypoint.sh"]
