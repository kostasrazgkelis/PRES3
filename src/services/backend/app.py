"""
    The backend endpoints of our web application for the service A (Alice)
"""
from flask import Flask, flash, request, redirect, render_template, json
import os
from werkzeug.utils import secure_filename
import pandas as pd
import requests
from settings import HOST, PORT, \
    ENVIRONMENT_DEBUG, SPARK_DISTRIBUTED_FILE_SYSTEM, \
    UPLOAD_FOLDER_A, UPLOAD_FOLDER_B, \
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
    return render_template('main.html')


@app.route('/about/', methods=['GET'])
@cross_origin()
def about():
    return render_template('about.html')


@app.route("/upload-files/", methods=["POST"])
@cross_origin()
def upload_file():
    print('asd')
    if request.method == "POST":
        if 'uploadedFile_A' not in request.files or 'uploadedFile_B' not in request.files:
            flash('No file part')
            return redirect(request.url)

        file_a = request.files['uploadedFile_A']
        file_b = request.files['uploadedFile_B']

        if file_a.filename == '' or file_b.filename == '':
            flash('No selected file')
            return redirect(request.url)

        if file_a and allowed_file(file_a.filename) and file_b and allowed_file(file_b.filename):
            filename = secure_filename(file_a.filename)
            file_a.save(os.path.join(app.config['UPLOAD_FOLDER_A'], filename))

            filename = secure_filename(file_b.filename)
            file_b.save(os.path.join(app.config['UPLOAD_FOLDER_B'], filename))

            response = app.response_class(
                status=200,
            )
            return response

        response = app.response_class(
            status=400,
        )
        return response


@app.route("/show_files/", methods=["GET"])
@cross_origin()
def show_files():
    # Return 404 if path doesn't exist
    if not os.path.exists(UPLOAD_FOLDER_A) or not os.path.exists(UPLOAD_FOLDER_B):
        response = app.response_class(
            status=400,
            mimetype='application/json'
        )
        return response

    # Show directory contents
    files_a = os.listdir(UPLOAD_FOLDER_A)
    files_b = os.listdir(UPLOAD_FOLDER_B)

    data_a = [get_data_from_file(UPLOAD_FOLDER_A, filename=file) for file in files_a]
    data_b = [get_data_from_file(UPLOAD_FOLDER_B, filename=file) for file in files_b]

    data = {'files_a': data_a, 'files_b': data_b}
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


def post_data(matching_field: str, noise: int, files: dict, cluster: str, port: int, values: dict):
    url = f'http://{cluster}:{port}/upload_data/'

    requests.post(url=url, files=files)
    response = requests.get(f'http://{cluster}:{port}/take_data/{matching_field}/{noise}', values)

    if response.status_code != 200:
        response = app.response_class(
            status=400,
            mimetype='application/json'
        )
        return response

    url_content = response.content
    with open(f"{SPARK_DISTRIBUTED_FILE_SYSTEM}/{cluster}_download.csv", 'wb') as file:
        file.write(url_content)

    response = app.response_class(
        status=200,
        mimetype='application/json'
    )
    return response


@app.route("/start/", methods=["POST"])
@cross_origin()
def start():
    response = request.get_json()

    noise = response['noise']
    matching_field = response['matching_field']
    prediction_size = response['prediction_size']

    file_name = response['file_a']['name']
    values = {"name": file_name, "columns": response['file_a']['columns']}
    files = {'upload_file': open(UPLOAD_FOLDER_A + "/" + file_name, 'rb')}
    response_1 = post_data(matching_field=matching_field,
                           noise=noise,
                           files=files,
                           cluster="cluster-a",
                           port=9200,
                           values=values)

    file_name = response['file_b']['name']
    values = {"name": file_name, "columns": response['file_b']['columns']}
    files = {'upload_file': open(UPLOAD_FOLDER_B + "/" + file_name, 'rb')}
    response_2 = post_data(matching_field=matching_field,
                           noise=noise,
                           files=files,
                           cluster="cluster-b",
                           port=9300,
                           values=values)

    if response_1.status_code == 200 or response_2.status_code == 200:
        spark = ThesisSparkClass()
        spark.main(matching_field="NCID",
                   prediction_size=prediction_size,
                   noise=noise)
        data = spark.get_metrics()

        spark.stop_spark()
        response = app.response_class(
            response=json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response

    response = app.response_class(
        status=400,
        mimetype='application/json'
    )
    return response



if __name__ == '__main__':
    app.config['UPLOAD_FOLDER_A'] = UPLOAD_FOLDER_A
    app.config['UPLOAD_FOLDER_B'] = UPLOAD_FOLDER_B
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)
