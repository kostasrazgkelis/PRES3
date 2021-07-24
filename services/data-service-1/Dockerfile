FROM python:3

WORKDIR /usr/src/app-1

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt


COPY . .

# define the port number the container should expose
EXPOSE 9200

CMD [ "python", "./app.py" ]