FROM python:3.9

WORKDIR /var/lib

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . /var/lib/

# define the port number the container should expose
EXPOSE 9200

CMD [ "python", "./app.py" ]