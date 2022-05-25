"""
    The backend endpoints of our web application for the service A (Alice)
"""
from flask import Flask, flash, send_file, request, redirect, render_template
import os
from werkzeug.utils import secure_filename
import requests

HOST = '0.0.0.0'
NAME_OF_CLUSTER = os.environ.get("NAME")
PORT = os.environ.get("PORT")
ENVIRONMENT_DEBUG = os.environ.get("DEBUG")

UPLOAD_FOLDER = f'./uploaded_files_{NAME_OF_CLUSTER}'
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET'])
def home():
    return render_template('main.html')

@app.route('/about/', methods=['GET'])
def about():
    return  render_template('about.html')

@app.route("/upload_files/", methods=["GET", "POST"])
def upload_file():

    if request.method == 'GET':
        return render_template('upload.html')

    if request.method == "POST":
        if 'uploadedFile' not in request.files:
            flash('No file part')
            return redirect(request.url)

        file = request.files['uploadedFile']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)

        if file and allowed_file(file.filename):
            # print(file)
            # resp = requests.post('http://cluster-a:9200/', files=file)
            # print(resp.content)
            return redirect(request.url)

            # return redirect(url_for('download_file', name=filename))

    return render_template('upload.html')

if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)