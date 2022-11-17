"""
    The backend endpoints of our web application for the service A (Alice)
"""
import os
import requests
from flask import Flask, request, json
import pandas as pd
from connector import HDFSConnector
from settings import HOST, PORT, \
    ENVIRONMENT_DEBUG, SPARK_DISTRIBUTED_FILE_SYSTEM, \
    ALLOWED_EXTENSIONS
from packages.spark_commands import ThesisSparkClass
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_data_from_file(directory: str, filename: str) -> dict:
    result_dict = {'name': None,
                   'columns': [None]}

    df = pd.read_csv(directory + "/" + filename, header=0)
    columns = df.columns.values.tolist()

    result_dict['name'] = filename
    result_dict['columns'] = columns
    return result_dict


@app.route('/', methods=['GET'])
def home():
    response = app.response_class(
        status=200
    )
    return response


@app.route("/show-files", methods=["GET"])
@cross_origin()
def show_files():
    # Return 404 if path doesn't exist
    if not os.path.exists(SPARK_DISTRIBUTED_FILE_SYSTEM + 'pretransformed_data') or \
            not os.path.exists(SPARK_DISTRIBUTED_FILE_SYSTEM + 'pretransformed_data'):
        response = app.response_class(
            status=404,
            mimetype='application/json'
        )
        return response

    # Show directory contents
    files_a = os.listdir(SPARK_DISTRIBUTED_FILE_SYSTEM + 'pretransformed_data')
    files_b = os.listdir(SPARK_DISTRIBUTED_FILE_SYSTEM + 'pretransformed_data')

    data_a = [get_data_from_file(SPARK_DISTRIBUTED_FILE_SYSTEM + 'pretransformed_data', filename=file)
              for file in files_a if file.endswith('.csv')]

    data_b = [get_data_from_file(SPARK_DISTRIBUTED_FILE_SYSTEM + 'pretransformed_data', filename=file)
              for file in files_b if file.endswith('.csv')]

    data = {'files_a': data_a, 'files_b': data_b}
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/start", methods=["POST"])
@cross_origin()
def start():
    response = request.get_json()

    prediction_size = response.get('prediction_size')
    project_name = response.get('project_name')
    cluster_a_file = response['file_a']['name']
    cluster_b_file = response['file_b']['name']

    hdfs_obj = HDFSConnector()

    if not hdfs_obj.check_hdfs():
        app.logger.error('The HDFS is not connected')
        response = app.response_class(
            status=500,
            message=json.dumps({"message": "The HDFS did not respond."})
        )
        return response

    app.logger.info("Connected to HDFS.")

    # cluster_a_file = hdfs_obj.download_file(directory='cluster_a_pretransformed_data', file=cluster_a_file)
    # cluster_b_file = hdfs_obj.download_file(directory='cluster_b_pretransformed_data', file=cluster_b_file)

    spark = ThesisSparkClass(project_name=project_name,
                             file_a=cluster_a_file,
                             file_b=cluster_b_file,
                             prediction_size=prediction_size)

    spark.start_etl()

    data = {
        "message": 'The join operation has finished.'
    }

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


if __name__ == '__main__':
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)
