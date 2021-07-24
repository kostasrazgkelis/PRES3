from flask import Flask, jsonify, request, render_template , Response, make_response
import os
import csv
import jellyfish
import io



HOST = '0.0.0.0'
PORT = '9300'
NAME_OF_CLUSTER = "Cluster_B"
app = Flask(__name__)
    
name= "asdasd"

list_with_data = [
    "Greece , GR",
    "Portugal, PRT",
    "Italy, ITA",
    "Great Britain, GB",
    "Albania, ALB",
    "Spain, ESP",
    "Germany, GRM",
    "Turkey, TRK"
    
]


def encode_with_soundex(list_with_data):
    for x in range(len(list_with_data)):
        country , short = list_with_data[x].split(',')
        country = jellyfish.soundex(str(country))
        short = jellyfish.soundex(str(short))
        list_with_data[x] = country+ ", " + short


    

@app.route('/')
def get():
    return f'{NAME_OF_CLUSTER}', 200

@app.route("/take_data/", methods=["GET"])
def post():
    print(f"{NAME_OF_CLUSTER}- The download has started!")

    try:
        fieldnames = ['Country', 'Short']
        si = io.StringIO()
        cw = csv.writer(si)
        encode_with_soundex(list_with_data)
        
        for x in list_with_data:
            cw.writerow(x.split(','))

    except Exception as e:
        print(f"{NAME_OF_CLUSTER}- There was an error!")
    else:
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=cluster_B_export.csv"
        output.headers["Content-type"] = "text/csv"
        return output



if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)