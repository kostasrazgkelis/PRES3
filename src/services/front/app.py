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

UPLOAD_FOLDER = '/src/services/app/uploaded_files_a/'
UPLOAD_FOLDER_A = '/src/services/app/uploaded_files_a/'
UPLOAD_FOLDER_B = '/src/services/app/uploaded_files_b/'

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
            file_a.save(os.path.join(UPLOAD_FOLDER_A, filename))
            filename = secure_filename(file_b.filename)
            file_b.save(os.path.join(UPLOAD_FOLDER_B, filename))

            return render_template('upload.html')


    return render_template('upload.html')

@app.route("/show_files/", methods=["GET", "POST"])
def show_files():

    # Return 404 if path doesn't exist
    if not os.path.exists(UPLOAD_FOLDER_A) or not os.path.exists(UPLOAD_FOLDER_B):
        return os.abort(404)

    # Show directory contents
    files_a = os.listdir(UPLOAD_FOLDER_A)
    files_b = os.listdir(UPLOAD_FOLDER_B)

    files = {
                'files_a': files_a,
                'files_b': files_b,
        }

    return render_template('files.html', data=files)

if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)