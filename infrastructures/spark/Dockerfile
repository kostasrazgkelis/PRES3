FROM python:3.9

RUN apt update && \
    apt install openjdk-11-jdk scala git python3-pip wget -y && \
    apt clean

RUN wget https://dlcdn.apache.org/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz && \
    tar xvf spark-* && \
    mv spark-3.2.2-bin-hadoop3.2 /opt/spark && \
    rm spark-*

COPY requirements.txt ./

RUN pip3 install -r requirements.txt


ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin