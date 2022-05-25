
FROM jupyter/all-spark-notebook:spark-3.2.1

USER root

COPY requirements.txt ./
COPY SparkCommands.ipynb ./

RUN pip3 install -r requirements.txt

EXPOSE 8888

CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]
