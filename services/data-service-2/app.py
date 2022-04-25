"""
    The backend endpoints of our web application
"""
from flask import Flask, send_file, request
import os
from flask.wrappers import Response
import pandas as pd
import packages.etl_pipeline as etl

HOST = '0.0.0.0'
PORT = '9300'
NAME_OF_CLUSTER = "Cluster_B"

app = Flask(__name__)

@app.route("/take_data/<matching_field>/", methods=['GET'])
@app.route("/take_data/<matching_field>/<int:noise>", methods=["GET"])
def get(matching_field=None , noise=0):
    """ The main get function that starts the ETL pipeline and return to Carol the encrypted data

    Returns:
        http_response: The file with the encrypted data
    """

    if  1000 <= noise <= 0:
        return None

    etl_object = etl.ETLModel(matching_field, noise)
    etl_object.start_etl()
    return send_file(f'/var/lib/data/hidden_information.csv',
                    mimetype='text/csv',
                    attachment_filename='b_cluster_data.csv',
                    as_attachment=True)

@app.route("/take_data/", methods=["POST"])
def post():
    try:
        downloaded_data = pd.read_csv(request.files['file'])
        downloaded_data.to_csv('/var/lib/data/datafile.csv', encoding='utf-8', index=False)  
    except FileExistsError:
        return Response(f"We could not find the file!")
    return Response(f"Save the file")


if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)