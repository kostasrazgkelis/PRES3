import os

HOST = '0.0.0.0'
NAME_OF_CLUSTER = os.environ.get("NAME")
PORT = os.environ.get("PORT")
ENVIRONMENT_DEBUG = os.environ.get("DEBUG")

UPLOAD_FOLDER = f'./uploaded_files_{NAME_OF_CLUSTER}'
ALLOWED_EXTENSIONS = {'csv'}
