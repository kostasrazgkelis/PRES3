


# Dockerfile
# base image
FROM ubuntu:16.04
# install packages

USER root

COPY requirements.txt ./


RUN apt-get update && \
    apt-get install -y curl \
    wget \
    openjdk-8-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

RUN apt-get update && apt-get -y update
RUN pip3 -q install pip --upgrade

RUN mkdir src
WORKDIR /src
COPY . .

RUN pip3 install -r requirements.txt
RUN pip3 install jupyter
RUN pip3 install pyspark


USER $NB_UID


CMD ["jupyter", "notebook", "--port=8888" ,"--no-browser", "--ip=0.0.0.0", "--allow-root"]