FROM ubuntu:latest
USER root

COPY requirements.txt ./

RUN apt-get update && apt-get -y update
RUN apt-get install -y build-essential python3.6 python3-pip python3-dev
RUN pip3 -q install pip --upgrade
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install wget 
RUN pip3 install pyspark
RUN pip3 install jupyter
RUN curl -L https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip > ijava-kernel.zip
RUN unzip ijava-kernel.zip -d ijava-kernel \
  && cd ijava-kernel \
  && python3 install.py --sys-prefix
  
USER $NB_UID

CMD ["jupyter", "notebook", "--port=8888" ,"--no-browser", "--ip=0.0.0.0", "--allow-root"]