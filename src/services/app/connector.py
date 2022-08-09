import requests

from settings import NAME_OF_CLUSTER, HDFS


def upload_file(file_name):
    file = {'uploadedFile': (f'{NAME_OF_CLUSTER}_pretransformed_data.csv', open(file_name, 'rb').read())}
    response = requests.post(url=HDFS + 'upload-file', files=file)

    if response.status_code == 200:
        return True
    return False