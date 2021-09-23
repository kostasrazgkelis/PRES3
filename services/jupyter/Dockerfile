FROM ubuntu:latest
USER root

COPY requirements.txt ./



RUN apt-get update && apt-get install -y \
    python3-pip



RUN pip3 install wget 
RUN pip3 install pyspark
RUN pip3 install jupyter
RUN apt-get install openjdk-16-jdk && apt-get install -y

RUN pip3 install --no-cache-dir -r requirements.txt

USER $NB_UID
USER $NB_UID

CMD ["jupyter", "notebook", "--port=8888" ,"--no-browser", "--ip=0.0.0.0", "--allow-root"]