#Defines the image of which the container 
#is going to be based 
FROM python:3.9

ARG NAME
#The containers’ copies in its storage the 
#requirements.txt file from the local machine
#This file is filled with all necessary libraries
#that we need to install before the container’s launch

COPY ./infrastructures/app/requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

COPY ./src/services/app/packages /src/app/packages
COPY ./src/services/app/app.py /src/app/app.py
COPY ./src/services/app/__init__.py /src/app/__init__.py
COPY ./src/services/app/uploaded_files_${NAME} /src/app/uploaded_files_${NAME}

WORKDIR src/app/

#It defines the port number the container should expose

#Finally, in order our container to run the python 
#code we do it with final command
CMD [ "python3", "./app.py" ]

