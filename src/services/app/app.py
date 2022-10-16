"""
    The backend endpoints of our web application for the service A (Alice)
"""
import requests
from flask import Flask, flash, request, json, send_file
import os
from os.path import isfile, join

from connector import HDFSConnector
from packages.etl_pipeline import ThesisSparkClassETLModel, ThesisSparkClassCheckFake
from werkzeug.utils import secure_filename
from settings import HOST, PORT, ENVIRONMENT_DEBUG, UPLOAD_FOLDER, ALLOWED_EXTENSIONS, \
    SPARK_DISTRIBUTED_FILE_SYSTEM, NAME_OF_CLUSTER
from flask_cors import CORS, cross_origin
import pandas as pd

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


@app.route("/upload-file", methods=['GET', 'POST'])
@cross_origin()
def post():
    if request.method == 'GET':
        response = app.response_class(
            status=200,
        )
        return response

    if request.method == 'POST':
        if 'uploadedFile' not in request.files:
            flash('No file part')
            response = app.response_class(
                status=400,
                response=json.dumps({"message": "Invalid input."})

            )
            return response

        file = request.files['uploadedFile']
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        response = app.response_class(
            status=200,
            response=json.dumps({"message": "The file has been uploaded."})

        )
        return response

    response = app.response_class(
        status=400,
    )
    return response


@app.route("/hdfs", methods=["GET"])
@cross_origin()
def hdfs():
    # Return 404 if path doesn't exist
    if not os.path.exists(SPARK_DISTRIBUTED_FILE_SYSTEM):
        response = app.response_class(
            status=400,
            mimetype='application/json'
        )
        return response

    # Show directory contents
    response = requests.get('http://hdfs:9500/show-files', json={"directory": "joined_data"})
    joined_data = json.loads(response.content)['files'][0]['name']

    data = {
        'joined': joined_data,
    }

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/show-files", methods=["GET"])
@cross_origin()
def show_files():
    # Return 404 if path doesn't exist
    if not os.path.exists(UPLOAD_FOLDER):
        response = app.response_class(
            status=404,
            mimetype='application/json'
        )
        return response

    # Show directory contents

    onlyfiles = [f for f in os.listdir(UPLOAD_FOLDER) if isfile(join(UPLOAD_FOLDER, f))]
    data = {'files': [get_data_from_file(UPLOAD_FOLDER, filename=file) for file in onlyfiles]}

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/send-data/", methods=['GET'])
@cross_origin()
def send():
    response = request.args

    matching_field: str = response['matching_field']
    joined_data_filename: str = response['joined_data_filename']
    file_name: str = response['file_name']

    hdfs_obj = HDFSConnector()

    if not hdfs_obj.check_hdfs():
        app.logger.info("Could not connect to HDFS.")
        response = app.response_class(
            status=400
        )
        return response
    app.logger.info("Connected to HDFS.")

    # response = hdfs_obj.upload_file(path=UPLOAD_FOLDER, file_name=file_name)

    ##
    # etl_object = ThesisSparkClassCheckFake(hdfs=hdfs_obj,
    #                                        filename=file_name,
    #                                        joined_data_filename=joined_data_filename,
    #                                        matching_field=matching_field)
    # result = etl_object.start_etl()
    # if result:
    #     app.logger.info(f"There was an error: {result}")
    #     response = app.response_class(
    #         status=400,
    #         response=json.dumps({'message': 'There was an error.'})
    #     )
    #     return response

    path = f"{SPARK_DISTRIBUTED_FILE_SYSTEM}{NAME_OF_CLUSTER}_matched_data/"
    file = [f for f in os.listdir(path) if isfile(join(path, f)) and f.endswith('.csv')][0]

    path = join(path, file)
    app.logger.info(f"TEST WE ARE HERE {path}")

    return send_file(filename_or_fp=path,
                     attachment_filename=f'{NAME_OF_CLUSTER}_matched_data.csv',
                     as_attachment=True)


@app.route("/take-data", methods=["POST", "GET"])
@cross_origin()
def get():
    """ The main get function that starts the ETL pipeline and return to Carol the encrypted data
    Returns:
        http_response: The file with the encrypted data
    """
    if request.method == 'GET':
        response = app.response_class(
            status=200
        )
        return response

    response = request.get_json()
    try:
        noise = int(response['noise'])
        matching_field = response['matching_field']
        columns = response['file']['columns']
        file_name = response['file']['name']
    except Exception as e:
        missing_fields = [option for option in ["noise", "matching_field", "columns", "name"]
                          if option not in response or response['file']]
        response = app.response_class(
            status=400,
            response=json.dumps({"message": f"The are missing fields: {missing_fields} "})
        )
        return response

    hdfs_obj = HDFSConnector()

    if 1000 <= noise <= 0:
        response = app.response_class(
            status=400,
            response=json.dumps({"message": f"The noise must be bettween 0-1000%"})
        )
        return response

    if not os.path.exists(UPLOAD_FOLDER + file_name):
        response = app.response_class(
            status=404,
            response=json.dumps({"message": f"The file {file_name} was not found."})

        )
        return response

    if not hdfs_obj.check_hdfs():
        response = app.response_class(
            status=500,
            message=json.dumps({"message": f"The HDFS did not respond."})
        )
        return response
    app.logger.info("Connected to HDFS.")

    response = hdfs_obj.upload_file(path=UPLOAD_FOLDER, file_name=file_name)
    if 1:  # response.status_code == 200:
        #     # etl_object = ThesisSparkClassETLModel(hdfs=hdfs_obj,
        #     #                                       columns=columns,
        #     #                                       filename=file_name,
        #     #                                       matching_field=matching_field,
        #     #                                       noise=noise)
        #     #
        #     # etl_object.start_etl()
        #
        response = app.response_class(
            status=200,
            response=json.dumps({'message': 'File has been transformed.'})
        )
        return response


if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)
