from flask import Flask, json, send_file, request
import os
from flask.wrappers import Response
import jellyfish
import pandas as pd
import random
import hashlib
import requests
import numpy as np

HOST = '0.0.0.0'
PORT = '9200'
NAME_OF_CLUSTER = "Cluster_A"
test_path = f"C:/Users/Kostas Razg/Desktop/Πτυχιακή/MyThesis/myThesis/services/data-service-1/book_chapter_table_25p_200k_A.csv"
correct_path = f'/var/lib/data/datafile.csv'

app = Flask(__name__)


def create_alp():  
    return str(chr(random.randrange(65,90))) + str(chr(random.randrange(48,54))) + str(chr(random.randrange(48,54))) + str(chr(random.randrange(48,54)))

def create_noise(noise, size, name):
    return pd.Series([create_alp() for _ in range(int(noise * size / 100))], dtype = 'string', name=name)

def create_fake_index(noise,size, name):
    return pd.Series(['Fake Index' for _ in range(int(noise * size / 100))], dtype = 'string', name=name)

def create_fake_soundex_values(noise, size, columns):
    a = create_noise(noise, size, columns[1])
    b = create_noise(noise, size, columns[2])
    fake_index = create_fake_index(noise, size, columns[0])

    return pd.DataFrame(pd.concat([fake_index, a, b], axis=1))

@app.route('/')
def get():
    return Response(200)

@app.route("/take_data/", methods=["POST"])
@app.route("/take_data/<matching_field>/", methods=['GET'])
@app.route("/take_data/<matching_field>/<int:noise>", methods=["GET"])
def post(matching_field=None, noise=None):

    columns_we_care = ['NCID', 'last_name', 'first_name']
    if request.method == 'GET' and 0 <= noise <= 1000:

        """
        Start Extracting the data 
        """
        data_frame = pd.read_csv(correct_path, header=0, delimiter=';"', engine='python').astype('string')


        """
        Finished Extracting the data 
        """

        """
        Start Transforming the data 
        """
        for column in data_frame.columns:
            data_frame.rename({column : column.strip('"')}, axis=1, inplace=True)

        
        for field in data_frame.columns:
            if field not in columns_we_care:
                data_frame.drop(columns = [field], inplace=True)
            else:
                data_frame[field] = data_frame[field].str.strip('"')

        columns_we_care.remove(matching_field)

        fake_soundex_values = create_fake_soundex_values(noise, data_frame.shape[0], data_frame.columns)

        for column in columns_we_care:
            #Applies to the data the jellyfish-soundex function and also encodes them with the SHA256 encryption
            data_frame[column] = data_frame[column].apply(lambda x: jellyfish.soundex(str(x))).apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )

            #Creates fake soundex values
            fake_soundex_values[column] = fake_soundex_values[column].apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )

        #We merge all the fake soundex data with the correct 
        merged_data = pd.concat([data_frame, fake_soundex_values], axis=0).sort_values(by=column)
        merged_data.to_csv('/var/lib/data/hidden_information.csv', encoding='utf-8', header=True, index=False)
        #We send back the final merged file

        """
        Finished Transforming the data 
        """
                
        return send_file(f'/var/lib/data/hidden_information.csv',
                        mimetype='text/csv',
                        attachment_filename='a_cluster_data.csv',
                        as_attachment=True)


    elif request.method == 'POST':

        try:
            downloaded_data = pd.read_csv(request.files['file'])
            downloaded_data.to_csv('/var/lib/data/datafile.csv', encoding='utf-8', index=False)  
        except FileExistsError as e:
            return Response(f"We could not find the file!")
        return Response(f"Save the file")


if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)