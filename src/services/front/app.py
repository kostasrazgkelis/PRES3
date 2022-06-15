"""
    The backend endpoints of our web application for the service A (Alice)
"""
from flask import Flask, flash, send_file, request, redirect, render_template, json
import os
from werkzeug.utils import secure_filename
import pandas as pd
import requests

HOST = '0.0.0.0'
NAME_OF_CLUSTER = os.environ.get("NAME")
PORT = os.environ.get("PORT")
ENVIRONMENT_DEBUG = os.environ.get("DEBUG")
UPLOAD_FOLDER_A = '/src/app/uploaded_files_cluster_a'
UPLOAD_FOLDER_B = '/src/app/uploaded_files_cluster_b'

ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)


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
def about():
    return render_template('about.html')


@app.route("/upload_files/", methods=["GET", "POST"])
def upload_file():
    if request.method == 'GET':
        return render_template('upload.html')

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

            return render_template('upload.html')
    return render_template('upload.html')


@app.route("/show_files/", methods=["GET", "POST"])
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
    result = requests.get(f'http://{cluster}:{port}/take_data/{matching_field}/{noise}', values)

    # response = app.response_class(
    #     status=200,
    #     mimetype='application/json'
    # )
    return result


@app.route("/start/", methods=["POST"])
def start():
    data = {"noise": 100,
            "matching_field": "NCID",
            "file_a": {'name': '50K_A.csv', 'columns': ["NCID",
                                                         "last_name",
                                                         "first_name",
                                                         "midl_name"]},
            "file_b": {'name': '50K_B.csv', 'columns': ["NCID",
                                                         "last_name",
                                                         "midl_name"]}}

    noise = data['noise']
    matching_field = data['matching_field']

    file_name = data['file_a']['name']
    values = {"name": file_name, "columns": data['file_a']['columns']}
    files = {'upload_file': open(UPLOAD_FOLDER_A + "/" + file_name, 'rb')}
    response_1 = post_data(matching_field=matching_field,
                           noise=noise,
                           files=files,
                           cluster="cluster-a",
                           port=9200,
                           values=values)

    file_name = data['file_b']['name']
    values = {"name": file_name, "columns": data['file_b']['columns']}
    files = {'upload_file': open(UPLOAD_FOLDER_B + "/" + file_name, 'rb')}
    response_2 = post_data(matching_field=matching_field,
                           noise=noise,
                           files=files,
                           cluster="cluster-b",
                           port=9300,
                           values=values)

    if response_1.status_code == 200 or response_2.status_code == 200:
        response = app.response_class(
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
