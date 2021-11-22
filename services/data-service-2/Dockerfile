FROM python:3

WORKDIR /usr/src/app-2

COPY requirements.txt ./
RUN pip install  -r requirements.txt


COPY . .

# define the port number the container should expose
EXPOSE 9300

CMD [ "python", "./app.py" ]