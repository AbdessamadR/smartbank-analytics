
FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root

RUN pip install --no-cache-dir delta-spark==4.4.1

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
