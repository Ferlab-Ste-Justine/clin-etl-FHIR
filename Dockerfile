FROM openjdk:8

ENV SPARK_VERSION=2.4.3
ENV HADOOP_VERSION=2.7
EXPOSE 8080 4040 7077 6066 8081

RUN wget --no-verbose https://www-us.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ADD scripts/clin-etl-submit.sh  clin-etl-submit.sh
ADD target/clin-etl.jar clin-etl.jar

ENTRYPOINT ["./clin-etl-submit.sh"]




