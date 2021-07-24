FROM jupyter/pyspark-notebook

USER root

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install wget \
    && pip3 install pyspark

USER $NB_UID

CMD jupyter notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root