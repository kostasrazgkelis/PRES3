"""
    The backend endpoints of our web application for the service A (Alice)
"""
from flask import Flask, flash, send_file, request, redirect, json
import os
from os.path import isfile, join
from packages.etl_pipeline import ThesisSparkClassETLModel
from werkzeug.utils import secure_filename
from settings import HOST, NAME_OF_CLUSTER, PORT, ENVIRONMENT_DEBUG, UPLOAD_FOLDER, ALLOWED_EXTENSIONS, SPARK_DISTRIBUTED_FILE_SYSTEM
from flask_cors import CORS, cross_origin
import pandas as pd
import shutil


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

@app.route("/upload-file", methods=['GET','POST'])
@cross_origin()
def post():
    if request.method == 'GET':
        response = app.response_class(
            status=200
        )
        return response

    if request.method == 'POST':
        if 'uploadedFile' not in request.files:
            flash('No file part')
            return redirect(request.url)

        file = request.files['uploadedFile']
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))


        response = app.response_class(
            status=200,
        )
        return response

    response = app.response_class(
    status=400,
    )
    return response

@app.route("/show-files", methods=["GET"])
@cross_origin()
def show_files():
    # Return 404 if path doesn't exist
    if not os.path.exists(UPLOAD_FOLDER):
        response = app.response_class(
            status=400,
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

    noise = int(response['noise'])
    matching_field = response['matching_field']
    columns = response['file_a']['columns']
    file_name = response['file_a']['name']
    
    if 1000 <= noise <= 0:
        response = app.response_class(
            status=400
        )
        return response

    if not matching_field:
        response = app.response_class(
            status=400
        )
        return response

    if not os.path.exists(UPLOAD_FOLDER + '/' + file_name):
        response = app.response_class(
            status=400
        )
        return response

    shutil.copy(UPLOAD_FOLDER + file_name, SPARK_DISTRIBUTED_FILE_SYSTEM + f'{NAME_OF_CLUSTER}_pretransformed_data')
    etl_object = ThesisSparkClassETLModel(file_name=file_name,
                                          columns=columns,
                                          matching_field=matching_field,
                                          noise=noise)
    etl_object.start_etl()

    
    response = app.response_class(
            status=200
        )
    return response


if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)