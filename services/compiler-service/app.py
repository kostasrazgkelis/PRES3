from flask import Flask, json, jsonify, request, render_template, Response
import requests
import os
import concurrent.futures
import time
from requests.sessions import CaseInsensitiveDict


app = Flask(__name__)

HOST = '0.0.0.0'
PORT = '9400'

def connect(URL):
    try:
        time_started = time.time()
        print(f"The download for the {URL} has started!!!!!!!!!!!!")
        request = requests.get(f'http://{URL}/take_data/')
        url_content = request.content
        name = requests.get(f'http://{URL}')
        csv_file = open(str(name.text) + "_download.csv", 'wb')
        csv_file.write(url_content)
        csv_file.close()
    except TimeoutError as e :
        print(f"There was a timeout Eroor {e}")
    else:
        print(f"The download for the {URL} has finished in {time.time() - time_started} second(s)!")
    finally:
        return request


@app.route("/connect_clusters", methods=["GET"])
def start_connection():

    URLs = [    
                'cluster-a:9200',
                'cluster-b:9300'
            ]
    
    try:

        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_table = {executor.submit(connect, url): url for url in URLs }

        for future in concurrent.futures.as_completed(future_table):
            try: 
                data = future.result()
            except Exception as e:
                print("there was an error")


    except IOError:
        print("I/O error")
        return Response("We failed for some reason", 400)
    else:
        return Response('<html><head><h1 style="background-color:powderblue;">The download has finished bravo kostas</h1></head></html>', 200)

if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)