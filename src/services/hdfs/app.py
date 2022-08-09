from flask import Flask, flash, request, redirect, json
import os
from os.path import isfile, join
from werkzeug.utils import secure_filename
from settings import HOST, PORT, ENVIRONMENT_DEBUG, SPARK_DISTRIBUTED_FILE_SYSTEM
from flask_cors import CORS, cross_origin
from utils import get_data_from_file
import logging

app = Flask(__name__)
CORS(app)

@app.route("/show-files", methods=["GET"])
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
    onlyfiles = [f for f in os.listdir(SPARK_DISTRIBUTED_FILE_SYSTEM) if isfile(join(SPARK_DISTRIBUTED_FILE_SYSTEM, f))]
    data = {'files': [get_data_from_file(SPARK_DISTRIBUTED_FILE_SYSTEM, filename=file) for file in onlyfiles]}

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route("/take-file", methods=["GET"])
@cross_origin()
def get():
    content = request.get_json(silent=True)
    file = content['file']
    if isfile(join(SPARK_DISTRIBUTED_FILE_SYSTEM, file)):
        data = {
            'file': file,
            'data': (file, open(join(SPARK_DISTRIBUTED_FILE_SYSTEM, file), 'rb').read())
        }
        response = app.response_class(
            response=json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response

    response = app.response_class(
        status=400,
    )
    return response

@app.route("/upload-file", methods=['GET', 'POST'])
@cross_origin()
def post():
    if request.method == 'GET':
        response = app.response_class(
            status=200
        )
        return response

    if request.method == 'POST':
        if 'uploadedFile' not in request.files:
            app.logger.warning("HDFS: The file was not found.")
            return redirect(request.url)

        file = request.files['uploadedFile']
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            app.logger.info("HDFS: FIle has been uploaded to the HDFS.")

        response = app.response_class(
            status=200,
        )
        return response

    app.logger.warning("HDFS: FIle was not uploaded to the HDFS.")
    response = app.response_class(
        status=400,
    )
    return response


if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = SPARK_DISTRIBUTED_FILE_SYSTEM
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)
