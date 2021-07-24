from flask import Flask, json, jsonify, request, render_template, make_response
import os
import jellyfish
import io
import csv


HOST = '0.0.0.0'
PORT = '9200'
NAME_OF_CLUSTER = "Cluster_A"

app = Flask(__name__)

list_with_data = [

    "Cyprus, CYP",
    "Bulgaria, BG",
    "Austria, AUS",
    "Russia, RUS",
    "United Stated of AMerica, USA",
    "Brazil, BRZ",
    "Argentina, ARG",
    "Romania, ROM"
    
]

def encode_with_soundex(list_with_data):
    for x in range(len(list_with_data)):
        country , short = list_with_data[x].split(',')
        country = jellyfish.soundex(str(country))
        short = jellyfish.soundex(str(short))
        list_with_data[x] = country+ ", " + short

       
#asd
@app.route('/')
def get():
    return f'{NAME_OF_CLUSTER}', 200


@app.route("/take_data/", methods=["GET"])
def post():
    print(f"{NAME_OF_CLUSTER}- Data is being send")
    try:
        fieldnames = ['Country', 'Short']
        si = io.StringIO()
        cw = csv.writer(si)
        encode_with_soundex(list_with_data)
        
        for x in list_with_data:
            cw.writerow(x.split(','))

    except Exception as e:
        print(f"{NAME_OF_CLUSTER} - There was an error!")
    else:
        print(f"{NAME_OF_CLUSTER}- the download has finished")
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=cluster_A_export.csv"
        output.headers["Content-type"] = "text/csv"
        return output

if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)