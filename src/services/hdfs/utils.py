from os.path import join

import pandas as pd
from settings import ALLOWED_EXTENSIONS


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_data_from_file(directory: str, filename) -> dict:
    result_dict = {'name': None,
                   'columns': [None]}

    df = pd.read_csv(join(directory, filename), header=0)
    columns = df.columns.values.tolist()

    result_dict['name'] = filename
    result_dict['columns'] = columns
    return result_dict
