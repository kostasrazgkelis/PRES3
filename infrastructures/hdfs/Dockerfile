#Defines the image of which the container
#is going to be based
FROM python:3.9


COPY ./infrastructures/hdfs/requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

COPY ./src/services/hdfs/app.py /src/app/app.py
COPY ./src/services/hdfs/settings.py /src/app/settings.py
COPY ./src/services/hdfs/utils.py /src/app/utils.py

WORKDIR /src/app/

#It defines the port number the container should expose

#Finally, in order our container to run the python
#code we do it with final command
CMD [ "python3", "./app.py" ]
