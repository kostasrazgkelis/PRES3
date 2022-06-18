"""
    The backend endpoints of our web application for the service A (Alice)
"""
import json
import requests
from flask import Flask, flash, send_file, request, redirect
import os
import packages.etl_pipeline as etl
from werkzeug.utils import secure_filename
from settings import HOST, NAME_OF_CLUSTER, PORT, ENVIRONMENT_DEBUG, UPLOAD_FOLDER, ALLOWED_EXTENSIONS

app = Flask(__name__)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/", methods=['GET'])
def home():
    print('asdasdasd')
    return "asdasd"


@app.route("/upload_data/", methods=['POST'])
def post():
    if request.method == 'GET':
        response = app.response_class(
            status=400,
            mimetype='application/json'
        )
        return response

    if request.method == 'POST':
        file = request.files['upload_file']
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        data = {'correct': True}
        response = app.response_class(
            response=json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route("/take_data/<matching_field>/", methods=['GET'])
@app.route("/take_data/<matching_field>/<int:noise>", methods=["GET"])
def get(matching_field=None, noise=0):
    """ The main get function that starts the ETL pipeline and return to Carol the encrypted data

    Returns:
        http_response: The file with the encrypted data
    """
    columns = request.args.getlist('columns')
    file_name = request.args.get(key='name')
    if 1000 <= noise <= 0:
        response = app.response_class(
            status=400,
            mimetype='application/json'
        )
        return response

    if not os.path.exists(UPLOAD_FOLDER + '/' + file_name):
        response = app.response_class(
            status=400,
            mimetype='application/json'
        )
        return response

    etl_object = etl.ETLModel(file_name=file_name,
                              columns=columns,
                              matching_field=matching_field,
                              noise=noise)
    etl_object.start_etl()

    return send_file(f'{UPLOAD_FOLDER}/clean_data/transformed_data.csv',
                     mimetype='text/csv',
                     attachment_filename=f'{NAME_OF_CLUSTER}.csv',
                     as_attachment=True)


if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)