import os
import time
import requests
from settings import HDFS


class HDFSConnector:

    def __init__(self, hdfs_base_url=HDFS):
        self.HDSF_BASE_URL = hdfs_base_url
        self.active = None
        self.file = None

    def get_hdfs_url(self):
        return self.HDSF_BASE_URL

    def check_hdfs(self) -> bool:
        return self.make_get_request(url=self.get_hdfs_url())

    def download_file(self, directory, file):
        url = self.HDSF_BASE_URL.join(f'{directory}/take-file?file={file}')
        response = requests.get(url=url)
        return open(response.content, "rb")

    def upload_file(self, path, file_name):
        files = {'uploadedFile': (file_name, open(os.path.join(path, file_name), 'rb').read())}
        url = HDFS + 'upload-file'
        return requests.post(url=url, files=files)

    def make_get_request(self, url, json_request=None):
        if json_request is None:
            json_request = {}
        count = 1
        while True:
            response = requests.get(url=url, json=json_request)
            if response.status_code == 200:
                return True
            count += count
            time.sleep(secs=count)

            if count > 10:
                break
        return False
