ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.3.2

RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
RUN pip --no-cache-dir install pandas
RUN pip --no-cache-dir install ipykernel

# Tambahkan dependensi Kafka
ARG KAFKA_VERSION=0.10.2.1
ARG SCALA_VERSION=2.12

RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${PYSPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${PYSPARK_VERSION}.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar

ENTRYPOINT ["bash"]
