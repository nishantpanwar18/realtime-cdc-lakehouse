FROM apache/hive:3.1.3

USER root

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/hive/lib
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.4/hadoop-aws-3.2.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

COPY core-site.xml /opt/hive/conf/core-site.xml

WORKDIR /opt/hive
USER hive

CMD ["/opt/hive/bin/hive", "--service", "metastore"]
