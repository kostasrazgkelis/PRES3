# Dockerfile
# base image
FROM ubuntu:16.04
# install packages
RUN apt-get update && \
    apt-get install -y curl \
    wget \
    openjdk-8-jdk
    
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java