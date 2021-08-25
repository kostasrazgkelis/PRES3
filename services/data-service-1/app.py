from flask import Flask, json, jsonify, request, render_template, make_response , send_file
import os
from flask.wrappers import Response
import jellyfish
import pandas as pd
from pandas.core.frame import DataFrame


HOST = '0.0.0.0'
PORT = '9200'
NAME_OF_CLUSTER = "Cluster_A"

app = Flask(__name__)


def encode_with_soundex(file):
    df1 = pd.read_csv(file)
    df1['soundex'] = df1['data1'].apply(lambda x: jellyfish.soundex(x))
 
@app.route('/')
def get():
    return f'{NAME_OF_CLUSTER}', 200


@app.route("/take_data/", methods=["GET"])
def post():
    print(f"{NAME_OF_CLUSTER}- Data is being send")

    try:
        df1 = pd.read_csv("/var/lib/data/A_1k_names_separated.csv", header=0, names=[0,1,2])

        column_1 = df1[0].apply(lambda x: jellyfish.soundex(x))
        column_2 = df1[1].apply(lambda x: jellyfish.soundex(x))
        column_3 = df1[2].apply(lambda x: jellyfish.soundex(x))

    except Exception as e:
        print(f"{NAME_OF_CLUSTER} - There was an error!")
    else:
        # Merge both datasets
        result = pd.concat([column_1, column_2, column_3], axis=1)

        result.to_csv('\var\lib\data\joined_data.csv', encoding='utf-8', index=False)
        
        print(f"{NAME_OF_CLUSTER}- the download has finished")

        return send_file('joined_data.csv',
                    mimetype='text/csv',
                    attachment_filename='a_cluster_data.csv',
                    as_attachment=True)

    

if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)