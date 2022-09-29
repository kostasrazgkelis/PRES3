#Defines the image of which the container
#is going to be based
FROM python:3.9


COPY ./infrastructures/hdfs/requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

COPY ./src/services/hdfs/app.py /src/app/app.py
COPY ./src/services/hdfs/settings.py /src/app/settings.py
COPY ./src/services/hdfs/utils.py /src/app/utils.py

WORKDIR /src/app/
RUN mkdir -p -v /opt/workspace/input
RUN mkdir -p -v /opt/workspace/joined_data
RUN mkdir -p -v /opt/workspace/cluster_a_matched_data
RUN mkdir -p -v /opt/workspace/cluster_b_matched_data
RUN mkdir -p -v /opt/workspace/cluster_a_pretransformed_data
RUN mkdir -p -v /opt/workspace/cluster_b_pretransformed_data

#Upload files for testing purposes
COPY ./infrastructures/hdfs/static_files/test_book_A.csv /opt/workspace/input/test_book_A.csv
COPY ./infrastructures/hdfs/static_files/test_book_B.csv /opt/workspace/input/test_book_B.csv
COPY ./infrastructures/hdfs/static_files/test_book_A.csv /opt/workspace/cluster_a_matched_data/test_book_A.csv
COPY ./infrastructures/hdfs/static_files/test_book_B.csv /opt/workspace/cluster_b_matched_data/test_book_B.csv
COPY ./infrastructures/hdfs/static_files/test_book_A.csv /opt/workspace/cluster_a_pretransformed_data/test_book_A.csv
COPY ./infrastructures/hdfs/static_files/test_book_B.csv /opt/workspace/cluster_b_pretransformed_data/test_book_B.csv

#Finally, in order our container to run the python
#code we do it with final command
CMD [ "python3", "./app.py" ]
