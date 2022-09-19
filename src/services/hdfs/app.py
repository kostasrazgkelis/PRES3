from flask import Flask, flash, request, redirect, json, make_response, jsonify, send_file
import os
from os.path import isfile, join
from werkzeug.utils import secure_filename
from settings import HOST, PORT, ENVIRONMENT_DEBUG, SPARK_DISTRIBUTED_FILE_SYSTEM, NAME_OF_CLUSTER
from flask_cors import CORS, cross_origin
from utils import get_data_from_file, allowed_file

app = Flask(__name__)
CORS(app)

@app.route("/", methods=["GET"])
@cross_origin()
def main():
    app.logger.info('A connection has been established.')
    data = {
        'message': 'The HDFS is active.'
    }

    response = app.response_class(
        status=200,
        response=json.dumps(data),
        mimetype='application/json'
    )
    return response


@app.route("/show-files", methods=["GET"])
@cross_origin()
def hdfs():
    args = request.args.to_dict()
    if ('directory' not in args) and (args['directory'] not in ['joined_data', 'matched_data', 'cluster_a_matched_data', 'cluster_b_matched_data', 'cluster_a_pretransformed_data', 'cluster_b_pretransformed_data']):
        response = app.response_class(
            status=404,
            response=json.dumps({'message': 'Invalid Input. The dir does not exist'}),
            mimetype='application/json'
        )
        return response

    # Return 404 if path doesn't exist
    directory = join(SPARK_DISTRIBUTED_FILE_SYSTEM + args['directory'])

    if not os.path.exists(directory):
        response = app.response_class(
            status=404,
            response=json.dumps({'message': 'The files does not exist'}),
            mimetype='application/json'
        )
        return response

    # Show directory contents

    files = [get_data_from_file(directory=directory, filename=file)
             for file in [f for f in os.listdir(directory)
                          if isfile(join(directory, f)) and allowed_file(f)]]

    data = {
        'files': files
    }

    response = app.response_class(
        status=200,
        response=json.dumps(data),
        mimetype='application/json'
    )
    return response


@app.route("/take-file/<directory>", methods=["GET"])
@cross_origin()
def get(directory):
    if directory not in ['joined_data', 'cluster_a_pretransformed_data', 'cluster_b_pretransformed_data']:
        response = app.response_class(
            status=400,
            response=json.dumps({'message': 'Wrong directory specified. Choose joined_data/cluster_a_pretransformed_data/cluster_b_pretransformed_data'}),
        )
        return response

    content = request.get_json(silent=True)
    if 'file' not in content:
        response = app.response_class(
            status=400,
            response=json.dumps({'message': 'File not specified'}),
        )
        return response

    file = content['file']

    path = join(join(SPARK_DISTRIBUTED_FILE_SYSTEM, directory), file)
    if isfile(path):
        return send_file(filename_or_fp=path, as_attachment=True)

    response = app.response_class(
        status=400,
        response=json.dumps({'message': 'File does not exist.'}),
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
            response = app.response_class(
                status=404,
                response=json.dumps({'message': 'The file was not found.'})
            )
            return response

        file = request.files['uploadedFile']
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
    app.config['UPLOAD_FOLDER'] = SPARK_DISTRIBUTED_FILE_SYSTEM + 'input'
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)
