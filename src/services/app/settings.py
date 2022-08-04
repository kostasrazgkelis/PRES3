import os

HOST = '0.0.0.0'
NAME_OF_CLUSTER = os.environ.get("NAME")
PORT = os.environ.get("PORT")
ENVIRONMENT_DEBUG = os.environ.get("DEBUG")
SPARK_DISTRIBUTED_FILE_SYSTEM = '/opt/workspace/'
UPLOAD_FOLDER = f'/src/app/uploaded_files_{NAME_OF_CLUSTER}/'
ALLOWED_EXTENSIONS = {'csv'}