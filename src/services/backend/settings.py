import os

HOST = '0.0.0.0'
NAME_OF_CLUSTER = os.environ.get("NAME")
PORT = os.environ.get("PORT")
ENVIRONMENT_DEBUG = os.environ.get("DEBUG")
SPARK_DISTRIBUTED_FILE_SYSTEM = '/opt/workspace/'
UPLOAD_FOLDER_A = '/src/app/uploaded_files_cluster_a'
UPLOAD_FOLDER_B = '/src/app/uploaded_files_cluster_b'
ALLOWED_EXTENSIONS = {'csv'}
