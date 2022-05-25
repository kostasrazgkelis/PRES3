"""
    The backend endpoints of our web application for the service A (Alice)
"""
from flask import Flask, flash, send_file, request, redirect
import os
import packages.etl_pipeline as etl
from werkzeug.utils import secure_filename

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

@app.route("/", methods=['GET'])
def home():
    print('asdasdasd')
    return "asdasd"


@app.route("/take_data/<matching_field>/", methods=['GET'])
@app.route("/take_data/<matching_field>/<int:noise>", methods=["GET"])
def get(matching_field=None , noise=0):
    """ The main get function that starts the ETL pipeline and return to Carol the encrypted data

    Returns:
        http_response: The file with the encrypted data
    """
    if  1000 <= noise <= 0:
        return None

    etl_object = etl.ETLModel(matching_field, noise)
    etl_object.start_etl()
    return send_file(f'/uploaded/hidden_information.csv',
                    mimetype='text/csv',
                    attachment_filename='a_cluster_data.csv',
                    as_attachment=True)

@app.route("/upload_file/", methods=["GET","POST"])
def upload_file():
    print('asd')
    if request.method == "POST":
        if 'uploadedFile' not in request.files:
            flash('No file part')
            return redirect(request.url)

        file = request.files['uploadedFile']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return  redirect(request.url)

            #return redirect(url_for('download_file', name=filename))

    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=file name=file_2>
      <input type=submit value=Upload>
    </form
    
    '''


if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['SECRET_KEY'] = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)